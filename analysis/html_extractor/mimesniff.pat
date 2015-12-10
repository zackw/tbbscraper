# 7.1 Identifying a resource with an unknown MIME type, table 1
3C21444F43545950452048544D4CTT  FFFFDFDFDFDFDFDFDFFFDFDFDFDFFF  Whitespace      text/html
3C48544D4CTT                    FFDFDFDFDFFF                    Whitespace      text/html
3C48454144TT                    FFDFDFDFDFFF                    Whitespace      text/html
3C534352495054TT                FFDFDFDFDFDFDFFF                Whitespace      text/html
3C494652414D45TT                FFDFDFDFDFDFDFFF                Whitespace      text/html
3C4831TT                        FFDFFFFF                        Whitespace      text/html
3C444956TT                      FFDFDFDFFF                      Whitespace      text/html
3C464F4E54TT                    FFDFDFDFDFFF                    Whitespace      text/html
3C5441424C45TT                  FFDFDFDFDFDFFF                  Whitespace      text/html
3C41TT                          FFDFFF                          Whitespace      text/html
3C5354594C45TT                  FFDFDFDFDFDFFF                  Whitespace      text/html
3C5449544C45TT                  FFDFDFDFDFDFFF                  Whitespace      text/html
3C42TT                          FFDFFF                          Whitespace      text/html
3C424F4459TT                    FFDFDFDFDFFF                    Whitespace      text/html
3C4252TT                        FFDFDFFF                        Whitespace      text/html
3C50TT                          FFDFFF                          Whitespace      text/html
3C212D2DTT                      FFFFFFFFFF                      Whitespace      text/html
3C3F786D6C                      FFFFFFFFFF                      Whitespace      text/xml
255044462D                      FFFFFFFFFF                      None            application/pdf
# 7.1 table 2
252150532D41646F62652D          FFFFFFFFFFFFFFFFFFFFFF          None            application/postscript
FEFF0000                        FFFF0000                        None            text/plain
FFFE0000                        FFFF0000                        None            text/plain
EFBBBF00                        FFFFFF00                        None            text/plain
# 6.1 Matching an image type pattern
00000100                        FFFFFFFF                        None            image/x-icon
00000200                        FFFFFFFF                        None            image/x-icon
424D                            FFFF                            None            image/bmp
474946383761                    FFFFFFFFFFFF                    None            image/gif
474946383961                    FFFFFFFFFFFF                    None            image/gif
5249464600000000574542505650    FFFFFFFF00000000FFFFFFFFFFFF    None            image/webp
89504E470D0A1A0A                FFFFFFFFFFFFFFFF                None            image/png
FFD8FF                          FFFFFF                          None            image/jpeg
# 6.2 Matching an audio or video type pattern
# Deliberately simplifies the 6.2.1 "Signature for MP4" mess down to something
# representable as a pattern match.
1A45DFA3                        FFFFFFFF                        None            video/webm
2E736E64                        FFFFFFFF                        None            audio/basic
464F524D0000000041494646        FFFFFFFF00000000FFFFFFFF        None            audio/aiff
494433                          FFFFFF                          None            audio/mpeg
4F67675300                      FFFFFFFFFF                      None            application/ogg
4D54686400000006                FFFFFFFFFFFFFFFF                None            audio/midi
524946460000000041564920        FFFFFFFF00000000FFFFFFFF        None            video/avi
524946460000000057415645        FFFFFFFF00000000FFFFFFFF        None            audio/wave
00000000667479706d7034          00000000FFFFFFFFFFFFFF          None            video/mp4
# 6.4 Matching an archive type pattern
1F8B08                          FFFFFF                          None            application/x-gzip
504B0304                        FFFFFFFF                        None            application/zip
526172201A0700                  FFFFFFFFFFFFFF                  None            application/x-rar-compressed
