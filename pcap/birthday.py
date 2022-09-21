def sum_bd():
  x = input()
  x_l = x.split('/')
  i = 0
  for e in x_l:
    i += int(e)
  return i