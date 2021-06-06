q1 = []
q2 = []
q3 = []
q4 = []
for x in range(1, 51):
    for y in range(50, 100):
        id_1 = (y*100)+x
        q1.append(id_1)

    for y_4 in range(0, 50):
        id_4 = (y_4*100)+x
        q4.append(id_4)

for x_2 in range(51, 101):
    for y_2 in range(50, 100):
        id_2 = (y_2*100)+x_2
        q2.append(id_2)

    for y_3 in range(0, 50):
        id_3 = (y_3*100)+x_2
        q3.append(id_3)

Q = q1.copy()
Q.extend(q2)
Q.extend(q3)
Q.extend(q4)
Q = sorted(Q)

# enter your desired cellid's here:
duomo = q2.index(5060)
navigli = q3.index(4456)
bocconi = q3.index(4259)
print(-1)