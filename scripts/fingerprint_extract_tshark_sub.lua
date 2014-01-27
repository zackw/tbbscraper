-- This is a subroutine of fingerprint_extract.py, invoked as a tshark
-- custom filter (tshark -q -Xlua_script:fingerprint_extract_tshark_sub.lua).
-- It does first-stage processing on the raw packet dump files, weeding out
-- uninteresting streams and decoding the remainder.  Due to the clunkiness
-- of this environment, we do as little work here as practical.
--
-- When this script is invoked, the environment variable SERVER_IP
-- must be set to the IP address of the server being monitored.

do
   local server_ip = os.getenv("SERVER_IP")
   if server_ip == nil then
      report_failure("SERVER_IP not set")
      os.exit(2)
   end

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
      local y = server_count % 26 + 1

      local label = string.rep(string.sub("ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                                          y, y), x)
      servers[addr] = label
      server_count = server_count + 1
      return label
   end

   local streams = {}
   local function packet_of_interest(stream, srcip, dstip, srcport, dstport)
      local port, direction

      if stream == nil then
         return false, srcport, "nostream"
      end

      if srcip == server_ip then
         port = srcport
         direction = "down"
      elseif dstip == server_ip then
         port = dstport
         direction = "up"
      else
         -- stray packet (should be impossible)
         port = dstport
         direction = "stray"
      end

      tag = streams[stream]
      if tag == nil then
         if direction == "up" then
            -- new connection to server; we care about this stream
            streams[stream] = true
            tag = true
         else
            -- outbound connection from server; ignore this stream
            streams[stream] = false
            tag = false
         end
      end
      return tag, port, direction
   end

   local fieldnames = {
      "tcp.len",
      "ip.src",
      "ip.dst",
      "tcp.srcport",
      "tcp.dstport",
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
      local stream      = nil
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
      for _, finfo in pairs(fieldlist) do
         if finfo.name == "ip.src" then
            srcip = tostring(finfo.value)

         elseif finfo.name == "ip.dst" then
            dstip = tostring(finfo.value)

         elseif (finfo.name == "tcp.srcport") then
            srcport = tonumber(finfo.value)

         elseif (finfo.name == "tcp.dstport") then
            dstport = tonumber(finfo.value)

         elseif (finfo.name == "tcp.len") then
            payload_len = tonumber(finfo.value)

         elseif finfo.name == "tcp.stream" then
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

      of_interest, port, direction = packet_of_interest(stream, srcip, dstip,
                                                        srcport, dstport)
      if not of_interest then return end

      if dupe then
         if flags == ""
         then flags = "dup"
         else flags = flags .. ".dup"
         end
      end
      sslrecords = table.concat(sslrecords, ".")
      sslreclens = table.concat(sslreclens, ".")
      io.write(string.format("%d:%.7f:%d:%d:%s:%s:%s:%d:%d:%s\n",
                             number,
                             time,
                             stream,
                             port,
                             direction,
                             flags,
                             sslrecords,
                             packet_len,
                             payload_len,
                             sslreclens))
   end
end
