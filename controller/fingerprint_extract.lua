do
   local function map(func, array)
      local new_array = {}
      for i,v in ipairs(array) do
         new_array[i] = func(v)
      end
      return new_array
   end

   -- http://ricilake.blogspot.com/2007/10/iterating-bits-in-lua.html
   local function hasbit(word, i)
      i = 2 ^ (i - 1)
      return word % (i + i) >= i
   end

   local function decode_tcp_flags(flags)
      local labels = { -- counting from the low end up
         "fin", "syn", "rst", "psh", "ack", "urg",
         "ece", "cwr", "ns",  "rsv1","rsv2","rsv3"
      }
      local decoded = ""
      for i, label in ipairs(labels) do
         if hasbit(flags, i) then
            if string.len(decoded) > 0 then
               decoded = decoded .. "."
            end
            decoded = decoded .. label
         end
      end
      return decoded
   end

   local rctypes = { [20] = "cipher",
                     [21] = "alert",
                     [22] = "encrypted-handshake",
                     [23] = "data",
                     [24] = "heartbeat" }

   local hstypes = { [0] = "hello-request",
                     [1] = "client-hello",
                     [2] = "server-hello",
                     [3] = "new-ticketed-session",
                     [11] = "certificate",
                     [12] = "server-key-exchange",
                     [13] = "certificate-request",
                     [14] = "server-done",
                     [15] = "certificate-verify",
                     [16] = "client-key-exchange",
                     [20] = "finished",
                     [22] = "certificate-status",
                     [67] = "next-proto" }

   local function decode_ssl_rctype(rctype, records)
      local label = rctypes[rctype]
      if label == nil then label = string.format("record-%d", rctype) end
      table.insert(records, label)
   end

   local function decode_ssl_hstype(hstype, records)
      local label = hstypes[hstype]
      if label == nil then label = string.format("handshake-%d", hstype) end
      local lastrecord = table.remove(records)
      if lastrecord ~= "encrypted-handshake" then
         table.insert(records, lastrecord)
      end
      table.insert(records, label)
   end

   local servers = {}
   local server_count = 0
   local function new_server(addr)
      local x = math.floor(server_count / 26) + 1
      local y = math.mod(server_count, 26) + 1

      local label = string.rep(string.sub("ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                                          y, y), x)
      servers[addr] = label
      server_count = server_count + 1
      return label
   end

   local fieldnames = {
      "tcp.len",
      "udp.length",
      "ip.src",
      "ip.dst",
      "tcp.srcport",
      "tcp.dstport",
      "udp.srcport",
      "udp.dstport",
      "tcp.flags",
      "tcp.stream",
      "tcp.analysis.retransmission",
      "tcp.analysis.duplicate_ack",
      "ssl.record.content_type",
      "ssl.record.length",
      "ssl.handshake.type"
   }
   local dissectors = map(Field.new, fieldnames)

   local ip = Listener.new("ip")
   function ip.packet(pinfo, tvb)
      local number      = tonumber(pinfo.number)
      local time        = tonumber(pinfo.abs_ts)
      local packet_len  = tonumber(pinfo.len)
      local stream      = 0
      local proto       = "udp"
      local flags       = ""
      local dupe        = false
      local payload_len = 0
      local srcip       = nil
      local dstip       = nil
      local srcport     = nil
      local dstport     = nil
      local direction   = nil
      local host        = nil
      local port        = nil
      local sslrecords  = {}
      local sslreclens  = {}

      local fieldlist = { all_field_infos() }
      for ix, finfo in ipairs(fieldlist) do
         if finfo.name == "ip.src" then
            srcip = tostring(finfo.value)

         elseif finfo.name == "ip.dst" then
            dstip = tostring(finfo.value)

         elseif (finfo.name == "tcp.srcport" or
                 finfo.name == "udp.srcport") then
            srcport = tonumber(finfo.value)

         elseif (finfo.name == "tcp.dstport" or
                 finfo.name == "udp.dstport") then
            dstport = tonumber(finfo.value)

         elseif (finfo.name == "tcp.len" or
                 finfo.name == "udp.length") then
            payload_len = tonumber(finfo.value)

         elseif finfo.name == "tcp.stream" then
            proto = "tcp"
            stream = tonumber(finfo.value) + 1

         elseif finfo.name == "tcp.flags" then
            flags = decode_tcp_flags(tonumber(finfo.value))

         elseif (finfo.name == "tcp.analysis.retransmission" or
                 finfo.name == "tcp.analysis.duplicate_ack") then
            dupe = true

         elseif finfo.name == "ssl.record.content_type" then
            decode_ssl_rctype(tonumber(finfo.value), sslrecords)

         elseif finfo.name == "ssl.handshake.type" then
            decode_ssl_hstype(tonumber(finfo.value), sslrecords)

         elseif finfo.name == "ssl.record.length" then
            table.insert(sslreclens, tonumber(finfo.value))

         end
      end

      host = servers[srcip..":"..srcport]
      if host ~= nil then
         direction = "down"
         port = srcport
      else
         host = servers[dstip..":"..dstport]
         if host == nil then host = new_server(dstip..":"..dstport) end
         direction = "up"
         port = dstport
      end

      if dupe then
         if flags == ""
         then flags = "dup"
         else flags = flags .. ".dup"
         end
      end

      sslrecords = table.concat(sslrecords, ".")
      sslreclens = table.concat(sslreclens, ".")
      io.write(string.format("%d:%d:%.6f:%s:%s:%s:%d:%s:%s:%d:%d:%s\n",
                             number,
                             stream,
                             time,
                             proto,
                             direction,
                             host,
                             port,
                             flags,
                             sslrecords,
                             packet_len,
                             payload_len,
                             sslreclens))
      io.flush()
   end
end
