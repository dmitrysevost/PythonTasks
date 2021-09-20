f_in = open('file_start.txt', 'r')
x_lines = f_in.readlines()
print(x_lines)
x_lines.reverse()
f_out = open('file_end.txt', 'w')
for i in range(len(x_lines)):
    f_out.write(x_lines[i][::-1])

f_out.close()
f_out = open('file_end.txt', 'r')
x_lines2 = f_out.readlines()
print(x_lines2)

f_in.close()
f_out.close()




