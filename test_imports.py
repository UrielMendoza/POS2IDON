from modules.Auxiliar import input_checker
flag, logs = input_checker()
print('inputs_flag:', flag)
for l in logs:
    print(' -', l)
