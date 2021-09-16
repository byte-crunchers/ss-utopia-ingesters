import csv

path = "C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/onethousand_users.csv"


class User:
    def __init__(self, user_id, user_name, email, password, f_name, l_name, is_admin):
        self.user_id = user_id
        self.user_name = user_name
        self.email = email
        self.password = password
        self.f_name = f_name
        self.l_name = l_name
        self.is_admin = is_admin

    def to_string(self):
        return str(
            str(self.user_id) + ", " + self.user_name + ", " + self.email + ", " + self.password + ", " + self.f_name +
            ", " + self.l_name + ", " + self.is_admin)


# Parse csv into users
def csv_to_users(file):
    objects = []
    with open(file, newline="") as user_file:
        reader = csv.reader(user_file, delimiter=',')
        row_count = 0
        for row in reader:
            # Functionality for ingesting users
            if len(row) == 7:
                try:
                    objects.append(
                        User(int(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6])))
                except ValueError:
                    print("Could not add user on line " + str(row_count) + ": " + str(row))
                    continue
    return objects


if __name__ == '__main__':
    csv_to_users("C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/onethousand_users.csv")
