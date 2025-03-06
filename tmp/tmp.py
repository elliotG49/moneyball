with open("/root/moneyball/tmp/country_code.txt", "r") as f:
    list = []
    for line in f:
        list.append(line.strip())
    print(set(list))
        