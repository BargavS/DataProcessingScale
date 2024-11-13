from unqlite import UnQLite
from math import cos, sin, sqrt, atan2, radians

db = UnQLite('sample.db')
data = db.collection('data')
business_values = data.filter(lambda user: "Buffets" in map(str.decode(), user['categories']))
print(map(str.decode(), ))


def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    business_values = collection.filter(lambda user: user['city'] == cityToSearch)
    with open(saveLocation1, "w") as file:
        for business in business_values:
            name = business['name']
            full_address = business['full_address'].replace("\n", ", ")
            city = business['city']
            state = business['state']
            file.write(name + "$" + full_address + "$" + city + "$" + state+ "\n")


true_results = ["VinciTorio's Restaurant$1835 E Elliot Rd, Ste C109, Tempe, AZ 85284$Tempe$AZ", "P.croissants$7520 S Rural Rd, Tempe, AZ 85283$Tempe$AZ", "Salt Creek Home$1725 W Ruby Dr, Tempe, AZ 85284$Tempe$AZ"]

try:
    FindBusinessBasedOnCity('Tempe', 'output_city.txt', data)
except NameError as e:
    print('The FindBusinessBasedOnCity function is not defined! You must run the cell containing the function before running this evaluation cell.')
except TypeError as e:
    print("The FindBusinessBasedOnCity function is supposed to accept three arguments. Yours does not!")

try:
    opf = open('output_city.txt', 'r')
except FileNotFoundError as e:
    print("The FindBusinessBasedOnCity function does not write data to the correct location.")

lines = opf.readlines()
if len(lines) != 3:
    print("The FindBusinessBasedOnCity function does not find the correct number of results, should be 3.")

lines = [line.strip() for line in lines]
if sorted(lines) == sorted(true_results):
    print( "Correct! You FindBusinessByCity function passes these test cases. This does not cover all possible test edge cases, however, so make sure that your function covers them before submitting!")
