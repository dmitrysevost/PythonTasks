f_in = open('file_start.txt', 'r')
x_lines = f_in.readlines()
print(x_lines)
x_lines.reverse()
f_out = open('file_end.txt', 'w')
for i in range(len(x_lines)):
    cur_string = x_lines[i]
    cur_length_of_string = len(cur_string)
    cur_out_string = ""
    for j in range(cur_length_of_string):
        cur_out_string = cur_out_string + cur_string[cur_length_of_string - j - 1]
    cur_out_string = cur_out_string
    f_out.write(cur_out_string)

f_out.close()
f_out = open('file_end.txt', 'r')
x_lines2 = f_out.readlines()
print(x_lines2)


f_in.close()
f_out.close()




