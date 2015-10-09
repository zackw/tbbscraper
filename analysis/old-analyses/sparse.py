class SparseList(list):
  def __setitem__(self, index, value):
    missing = index - len(self) + 1
    if missing > 0:
      self.extend(['0'] * missing)
    list.__setitem__(self, index, value)
  # def __getitem__(self, index):
    # try: return list.__getitem__(self, index)
    # except IndexError: return None