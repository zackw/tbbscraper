import word_seg.arabic
seg = word_seg.arabic.Segmenter()
print(repr(list(seg.segment("في_منتزه_الملك_عبدالعزيز_بالسودةfollow"))))
