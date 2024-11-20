local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print("discovered", item)
    target[item] = true
--local count = 0 for _ in pairs(target) do count = count + 1 end print("disco", count)
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs({
    ["^https?://ask%.fm/([0-9a-zA-Z_]+)$"]="user",
    ["^https?://ask%.fm/([0-9a-zA-Z_]+%?page=[0-9]+)$"]="user-page",
    ["^https?://ask%.fm/([^/]+/questions%?page=[0-9]+)$"]="user-page",
    ["^https?://ask%.fm/([^/]+/versus%?page=[0-9]+)$"]="user-page",
    ["^https?://ask%.fm/([^/]+/answers/[0-9]+)$"]="answer",
    ["^https?://ask%.fm/([^/]+/photopolls/[0-9]+)$"]="photopoll",
    ["^https?://ask%.fm/countries/([^/]+/shoutouts/[0-9]+)$"]="question",
    ["^https?://askfm%.site/([^/]+/threads/[0-9]+)$"]="thread",
    ["^https?://(c[a-z][a-z][a-z]%.ask%.fm/.+)$"]="asset",
    ["^https?://ask%.fm/tags/([^%?/]+)$"]="tag"
  }) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {
      ["find_max"]={},
      ["allow_question"]=false
    }
    item_type_ = found["type"]
    if item_type_ == "user" then
      newcontext["user"] = item_value_
      item_value_ = found["value"]
    elseif item_type_ == "user-page" then
      if not string.match(found["value"], "/") then
        newcontext["user"], newcontext["page"] = string.match(found["value"], "^([^%?]+)%?page=([0-9]+)$")
        newcontext["path"] = ""
      else
        newcontext["user"], newcontext["path"], newcontext["page"] = string.match(found["value"], "^([^/]+)/([a-z]+)%?page=([0-9]+)$")
      end
      item_value_ = newcontext["user"] .. ":" .. newcontext["path"] .. ":" .. newcontext["page"]
    elseif item_type_ == "answer"
      or item_type_ == "photopoll"
      or item_type_ == "thread"
      or item_type_ == "question" then
      newcontext["user"], newcontext["id"] = string.match(found["value"], "^([^/]+)/[a-z]+/([0-9]+)$")
      if ids[newcontext["id"]] then
        return nil
      end
      item_value_ = newcontext["user"] .. ":" .. newcontext["id"]
    elseif item_type_ == "asset"
      or item_type_ == "tag" then
      item_value_ = found["value"]
    else
      error("Should not reach this.")
    end
    item_type = item_type_
    item_value = item_value_
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      context = newcontext
      ids[string.lower(item_value)] = true
      if context["id"] then
        ids[string.lower(context["id"])] = true
      end
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      is_new_design = false
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  if ids[url]
    or context["find_max"][url] then
    return true
  end

  if string.match(url, "^https?://[^/]+/[^/]+/[a-z]+/[0-9]+/flag")
    or string.match(url, "^https?://[^/]+/[^/]+/answers/[0-9]+/likes$")
    or string.match(url, "^https?://[^/]+/[^/]+/answers/[0-9]+/rewards$")
    or string.match(url, "^https?://[^/]+/[^/]+/answers/[0-9]+/fans/likes$")
    or string.match(url, "^https?://[^/]+/[^/]+/answers/[0-9]+/fans/rewards$")
    or string.match(url, "^https?://[^/]+/[^/]+/answers/[0-9]+/fans/votes$")
    or string.match(url, "^https?://[^/]+/[^/]+/ask$")
    or string.match(url, "^https?://[^/]+/[^/]+/flag")
    or string.match(url, "^https?://[^/]+/[^/]+/answers/poll%?newer=[0-9]+$")
    or string.match(url, "^https?://[^/]*/account/private%-")
    or string.match(url, "^https?://[^/]+/[^/]+/best%?.*page=")
    or string.match(url, "[%?&]no_next_link=true")
    or (
      parenturl
      and string.match(url, "[%?&]iterator=")
      and (
        string.match(parenturl, "[%?&]iterator=")
        or string.match(parenturl, "[%?&]page=1")
      )
      and not (
        string.match(parenturl, "^https?://[^/]+/[^/]+/versus")
        or string.match(parenturl, "^https://[^/]+/countries/[a-z]+/shoutouts/[0-9]+$")
      )
    )
    or (
      item_type == "user"
      and (
        string.match(url, "^https?://[^/]+/[^/]+/answers?/[0-9]+$")
        --or string.match(url, "^https?://[^/]+/[^/]+/photopolls/[0-9]+$")
        or (
          string.match(url, "[%?&]iterator=[0-9]")
          and not string.match(url, "^https?://[^/]+/[^/]+/versus")
        )
      )
    ) then
    return false
  end

  local skip = false
  for pattern, type_ in pairs({
    ["^https?://ask%.fm/([0-9a-zA-Z_%-]+)"]="user",
    ["^https?://ask%.fm/([0-9a-zA-Z_%-]+)/?([a-z]*)%?page=([0-9]+)$"]="user-page",
    --["^https?://ask%.fm/([^/]+)/answer/([0-9]+)"]="answer",
    ["^https?://ask%.fm/countries/([^/]+)/shoutouts/([0-9]+)"]="question",
    ["^https?://ask%.fm/([^/]+)/photopolls/([0-9]+)"]="photopoll",
    ["^https?://askfm%.site/([^/]+)/threads/([0-9]+)"]="thread",
    ["^https?://ask%.fm/([^/]+)/threads/([0-9]+)"]="thread",
    ["^https?://(c[a-z][a-z][a-z]%.ask%.fm/.+)"]="asset",
    ["^https?://ask%.fm/tags/([^%?/]+)"]="tag"
  }) do
    match1, match2, match3 = string.match(url, pattern)
    if match1
      and (
        type_ ~= "user-page"
        or match2 == ""
        or match2 == "questions"
        --or match2 == "versus"
      ) and (
        (
          type_ ~= "question"
          and type_ ~= "answer"
          and type_ ~= "photopoll"
        )
        or not ids[match2]
      ) and (
        type_ ~= "question"
        or context["allow_question"]
      ) then
      if type_ == "question"
        or type_ == "photopoll"
        or type_ == "thread" then
        match = match1 .. ":" .. match2
      elseif type_ == "user-page" then
        match = match1 .. ":" .. match2 .. ":" .. match3
      else
        match = match1
      end
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name
        and (
          context["user"] ~= match
          or item_type ~= "user"
          or type_ == "photopoll"
        )
        and (
          type_ ~= "user"
          or match ~= "countries"
        ) then
        discover_item(discovered_items, new_item)
        if type_ ~= "user" then
          skip = true
        end
      end
    end
  end
  if skip then
    return false
  end

  if not string.match(url, "^https?://[^/]*ask%.fm/")
    and not string.match(url, "^https?://[^/]*askfm%.site/") then
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  end

  if ids[string.lower(string.match(url, "^https?://(.+)$"))] then
    return true
  end

  if item_type == "user-page"
    and string.match(url, "^https?://[^/]+/([^%?/]+)") == context["user"]
    and string.match(url, "[%?&]page=([0-9]+)") == context["page"] then
    return true
  end

  for _, pattern in pairs({
    "([a-z0-9A-Z_%-]+)",
    "([0-9]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end]]

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil
  local body_data = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  -- by lennier2
  local function clean_url(url)
    -- Remove any extra data appended directly after numeric IDs in 'answer' or 'answers' URLs,
    -- but keep valid path segments like '/fans/likes'.
    url = string.gsub(url, "^(https?://ask%.fm/[^/]+/answers?/[0-9]+)[^&/?#]*", "%1")
    return url
  end

  local function check(newurl)
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    url_ = clean_url(url_)
    if (
        string.match(url_, "/fans/likes%?")
        or string.match(url_, "/fans/rewards%?")
        or string.match(url_, "/fans/votes%?")
      ) and not string.match(url_, "%?page=[0-9]+$") then
      local page = string.match(url_, "[%?&](page=[0-9]+)")
      if page then
        --[[if string.match(url_, "/votes") then
          check("https://ask.fm/" .. context["user"] .. "/photopolls/" .. context["id"] .. "?" .. page)
        end]]
        return check(string.match(url_, "^([^%?]+%?)") .. page)
      else
        return nil
      end
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      local headers = {}
      if string.match(url_, "&no_prev_link=") then
        headers["X-Requested-With"]="XMLHttpRequest"
      end
      --[[if string.match(url_, "^https?://[^/]+/[^/]+/[a-z]+/[0-9]+/fans/likes.*[%?&]page=")
        or string.match(url_, "^https?://[^/]+/[^/]+/[a-z]+/[0-9]+/fans/rewards.*[%?&]page=")
        or string.match(url_, "^https?://[^/]+/[^/]+/[a-z]+/[0-9]+/fans/votes.*[%?&]page=")
        or string.match(url_, "^https?://[^/]+/countries/[a-z]+/shoutouts/[0-9]+.*[%?&]page=")
        or string.match(url_, "^https?://[^/]+/[^/]+/photopolls/[0-9]+.*[%?&]page=") then
        headers["X-Requested-With"]="XMLHttpRequest"
      end]]
      table.insert(urls, {
        url=url_,
        headers=headers
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      if default ~= nil then
        default = tostring(default)
      end
      return set_new_params(newurl, {[param]=default})
    end
  end

  local function queue_with_body(url, body)
    body_data = body
    check(url)
    body_data = nil
  end

  local function read_number(s)
    local num = ""
    for s in string.gmatch(s, "([0-9]+)") do
      num = num .. s
    end
    if string.len(num) == 0 then
      return 0
    end
    return tonumber(num)
  end

  local function calc_pages(count_s)
    return math.ceil(read_number(count_s)/25) + 1
  end

  local function queue_pages(partial_url, count)
    for i=1,count do
      check(partial_url .. tostring(i))
    end
  end

  local function find_max(newurl, base_url, start_step)
    if base_url then
      assert(not context["find_max"][newurl])
      context["find_max"][newurl] = {
        ["page"]=1,
        ["step"]=start_step,
        ["prev_url"]=base_url,
        ["exists"]=true
      }
      check(newurl)
      return nil
    elseif not context["find_max"][newurl] then
      return nil
    end
    local prev_data = context["find_max"][newurl]
    local prev_step = prev_data["step"]
    local prev_page = prev_data["page"]
    local prev_url = prev_data["prev_url"]
    local done = nil
    local next_url = nil
    if not string.match(html, "([^%s]+)") or not prev_data["exists"] then
      context["find_max"][newurl]["exists"] = false
      local next_step = 0
      if prev_step > 1 then
        next_step = math.ceil(prev_step/5)
      end
      if next_step == 0
        or prev_page == 1 then
        local page_num = string.match(newurl, "[%?&]page=([0-9]+)")
        local base_url = newurl
        while true do
          base_url = context["find_max"][base_url]["prev_url"]
          if not context["find_max"][base_url] then
            break
          end
        end
        assert(base_url ~= newurl)
        page_num = tonumber(page_num) - 1
        print("Found " .. tostring(page_num) .. " pages for " .. base_url)
        if page_num == 0 then
          return nil
        end
        return queue_pages(base_url, tonumber(page_num)-1)
      else
        local next_page = prev_page - prev_step + next_step
        next_url = set_new_params(newurl, {["page"]=tostring(next_page)})
        --next_url = increment_param(newurl, "page", 1, next_step-prev_step)
        done = context["find_max"][next_url]
        context["find_max"][next_url] = {
          ["page"]=next_page,
          ["step"]=next_step,
          ["prev_url"]=prev_url,
          ["exists"]=true
        }
      end
    else
      local next_page = prev_page + prev_step
      next_url = set_new_params(newurl, {["page"]=tostring(next_page)})
      --next_url = increment_param(newurl, "page", 1, prev_step)
      context["find_max"][newurl]["exists"] = true
      done = context["find_max"][next_url]
      context["find_max"][next_url] = {
        ["page"]=next_page,
        ["step"]=prev_step,
        ["prev_url"]=newurl,
        ["exists"]=true
      }
    end
    if done then
      context["find_max"][next_url]["exists"] = done["exists"]
      find_max(next_url)
    else
      check(next_url)
    end
  end

  if allowed(url)
    and status_code < 300
    and not string.match(url, "^https?://c[a-z][a-z][a-z]%.ask%.fm/.") then
    html = read_file(file)
    find_max(url)
    local id = string.match(url, "([0-9]+)$")

    if string.match(url, "^https?://[^/]*ask%.fm/[^/]+/answers/[0-9]+$") then
      for d in string.gmatch(html, '<header class="streamItem_header"[^>]+>(.-)</header>') do
        if not string.match(d, '<a class="author[^>]+href="') then
          local question_url = string.match(d, 'href="(https?://[^/]*/countries/[a-z]+/shoutouts/[0-9]+)"')
          if question_url then
            context["allow_question"] = true
            check(question_url)
            context["allow_question"] = false
          end
        end
      end
      for _, key in pairs({"like", "reward"}) do
        local count = string.match(html, '<a class="icon%-' .. key .. '"[^>]+>%s*</a>%s*<a class="counter"[^>]+href="[^"]+/' .. id .. '">([^<]+)')
        count = read_number(count)
        if count > 9 then
          check(url .. "/fans/" .. key .. "s?page=" .. 1)
          --queue_pages(url .. "/fans/" .. key .. "s?page=", count)
        end
      end

    elseif string.match(url, "^https?://[^/]*ask%.fm/[^/]+/photopolls/[0-9]+$") then
      local count = string.match(html, '<a class="voteCount"[^>]+href="[^"]+/' .. id .. '">([^<]+)')
      count = read_number(count)
      if count > 0 then
        -- yes with /answers/
        check(string.match(url, "^(https?://[^/]+/[^/]+/)") .. "answers/" .. id .. "/fans/votes?page=1")
        --queue_pages(url .. "?page=", count)
      end

    elseif string.match(url, "^https://askfm%.site/[^/]+/threads/([0-9]+)$") then
      check("https://ask.fm/" .. string.match(url, "^https?://[^/]+/(.+)$"))

    elseif string.match(url, "^https?://[^/]*ask%.fm/[^%?/]+%?page=[0-9]+$")
      or string.match(url, "^https?://[^/]*ask%.fm/[^/]+/questions%?page=[0-9]+$") then
      local found = 0
      local pattern_name = string.match(url, "^https?://[^/]+/[0-9a-zA-Z_%-]+/?([a-z]*)%?page=")
      local pattern = ({
        [""]='<a class="streamItem_meta" href="https?://ask%.fm/([^/]+)/answers/([0-9]+)">',
        ["questions"]='href="https?://ask%.fm/countries/([a-z]+)/shoutouts/([0-9]+)">'
      })[pattern_name]
      for user, id in string.gmatch(html, pattern) do
        if pattern_name == "questions"
          or user == context["user"] then
          found = found + 1
          ids[id] = true
        end
      end
      if found == 0 then
        abort_item()
        return nil
      end
      --assert(found > 0)

    elseif string.match(url, "^https?://[^/]*ask%.fm/[0-9a-zA-Z_%-]+$") then
      local answer_count = string.match(html, '<div title="([^"]+)" class="profileTabAnswerCount text%-large"')
      local like_count = string.match(html, '<div title="([^"]+)" class="profileTabLikeCount text%-large"')
      local base_url = url .. "?page="
      local answer_step_start = calc_pages(answer_count) + 1
      if answer_step_start > 1000 then
        answer_step_start = 0
      end
      --queue_pages(base_url, answer_count)
      find_max("https://ask.fm/" .. item_value .. "?page=1&no_prev_link=true", base_url, answer_step_start)
      find_max("https://ask.fm/" .. item_value .. "/questions?page=1&no_prev_link=true", url .. "/questions?page=", answer_step_start)
      --find_max("https://ask.fm/" .. item_value .. "/versus?page=1&no_prev_link=true", url .. "/versus?page=")
    end

    for data in string.gmatch(html, '(data%-params="{[^"]+"[^>]+)') do
      local href = string.match(data, 'href="([^"]+)"')
      local params = string.match(data, 'data%-params="([^"]+)"')
      if not href or not params then
        error("Incorrect form data extracted.")
      end
      params = cjson.decode(html_entities.decode(params))
      href = html_entities.decode(href)
      for k, v in pairs(params) do
        if type(v) ~= "string" then
          params[k] = tostring(v)
        end
      end
      check(set_new_params(urlparse.absolute(url, href), params))
    end

    html = string.gsub(html, 'data%-params="{[^"]+"[^>]+', '')
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200
    and not (
      http_stat["statcode"] == 301
      and (
        string.match(url["url"], "^https?://ask%.fm/[^/]+/threads/[0-9]")
        or string.match(url["url"], "^https?://ask%.fm/[^/]+/answer/[0-9]")
      )
    ) then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if is_new_design then
    return wget.actions.EXIT
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 5
    if status_code == 403 then
      if item_type == "asset" then
        maxtries = 0
      elseif string.match(url["url"], "^https?://ask%.fm/.") then
        maxtries = 10
      end
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["askfm-rojyushaevqzhbg7"] = discovered_items,
    ["urls-94gfdddny6p9k46p"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


