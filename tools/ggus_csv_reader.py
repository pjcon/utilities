import os.path

import pandas as pd


def generate_fields_dict(csv_file):
    first_line = csv_file.readline()
    field_dict = {}
    current_field_index = 0
    while first_line.count(";") >= 1:
        current_field = first_line[:first_line.find(";")]
        field_dict.update({current_field.lower(): current_field_index})
        current_field_index += 1
        first_line = first_line[first_line.find(";") + 1:]
    field_dict.update({"internal priority": current_field_index})
    field_dict.update({"internal owner": current_field_index + 1})
    return field_dict


def edit_ticket(id_list, fields_dict, edits_file_name):
    while True:
        editing = input("Which ticket should be edited? ")
        if editing in str(id_list):
            original_field = input("Which field should be edited? ").lower()
            if original_field in fields_dict:
                edited_field = input("What should the field " + original_field + " be changed to? ")
                if ";" not in edited_field:
                    if not os.path.isfile(edits_file_name):
                        open(edits_file_name, "w").close()
                    with open(edits_file_name, "r") as f:
                        file_read = f.read()
                        file_read = edit_value(editing, file_read, fields_dict, original_field, edited_field)
                    with open(edits_file_name, "w") as f:
                        f.write(file_read)
                    print("Successfully changed field %s in ticket %s to %s" % (original_field, editing, edited_field))
                    return
                else:
                    print("The new field may not include any semi-colons")
            else:
                print("Invalid ticket field")
        else:
            print("No valid ticket with this ID exists")


def edit_value(editing_ticket, file_read, fields_dict, original_field, edited_field):
    if str(editing_ticket) not in file_read:
        separators = ""
        for i in range(len(fields_dict)):
            separators = separators + ";"
        file_read = file_read + (str(editing_ticket) + separators + "\n")
    temp = file_read.find(str(editing_ticket))
    val = temp - 1
    for i in range(0, int(fields_dict[original_field])):
        val = file_read.find(";", val + 1)
    file_read_list = list(file_read)
    while file_read_list[val + 1] != ";":
        file_read_list.pop(val + 1)
    file_read_list[val] = ";" + edited_field
    file_read = "".join(file_read_list)
    return file_read


def overwrite(ticket_list, id_list, fields_dict, edits_file_name):
    overwritten_line = []
    overwritten_list = ticket_list
    if os.path.isfile(edits_file_name):
        with open(edits_file_name) as edit_file:
            edit_file_lines = len(edit_file.readlines())
            edit_file.seek(0)
            overwritten_list = ticket_list
            for i in range(edit_file_lines):
                current_line = edit_file.readline()
                current_id = int(current_line[:6])
                current_field_index = 0
                while current_line.count(";") >= 0 and current_field_index <= len(fields_dict):
                    current_field = current_line[:current_line.find(";")]
                    if current_field != "" or current_field_index >= len(fields_dict) - 2:
                        overwritten_line.append(current_field)
                    else:
                        overwritten_line.append(ticket_list[id_list.index(current_id)][current_field_index])
                    current_line = current_line[current_line.find(";") + 1:]
                    current_field_index += 1
                overwritten_list[id_list.index(current_id)] = overwritten_line[:]
                overwritten_line *= 0
    return overwritten_list


def console_display(overwritten_list, field_dict):
    sorted_list = sort_tickets(overwritten_list, field_dict)
    current_line = ""
    for i in range(len(field_dict)-2):
        current_line += {v: k for k, v in field_dict.items()}[i].ljust(105, " ")
    print(current_line.upper())
    current_line *= 0
    for i in range(len(sorted_list)):
        sorted_list_line = sorted_list[i]
        for j in range(len(sorted_list[i])):
            current_line += sorted_list_line[j].ljust(105, " ")
        print(current_line)
        current_line *= 0


def excel_display(overwritten_list, field_dict, csv_file_name, fields_dict):
    sorted_list = sort_tickets(overwritten_list, field_dict)
    pd.set_option('display.max_columns', None)
    df = pd.DataFrame(sorted_list[:-1])
    df_columns = pd.DataFrame([fields_dict.keys()])
    df = pd.concat([df_columns, df], axis=0)
    writer = pd.ExcelWriter(csv_file_name[:-4] + "_excel.xlsx")
    df.to_excel(writer, sheet_name="sheet1", index=False, header=False)
    writer.save()


def sort_tickets(overwritten_list, field_dict):
    direction_dict = {"ascending": False, "descending": True}
    while True:
        sort_field = input("By which field should the tickets be sorted? ").lower()
        if sort_field in field_dict:
            sort_direction = input("[ascending] or [descending]? ").lower()
            if sort_direction in direction_dict:
                sorted_list = sorted(overwritten_list, key=lambda x: x[field_dict[sort_field]],
                                     reverse=direction_dict[sort_direction.lower()])
                return sorted_list
            else:
                print("Invalid sorting direction")
        else:
            print("Invalid ticket field")


def main():
    csv_files_counter = 0
    for filename in os.listdir("."):
        if filename.endswith(".csv"):
            csv_files_counter += 1
            csv_file_path = filename
            csv_file_name = os.path.basename(filename)
    if csv_files_counter == 1:
        with open(csv_file_path, "r") as csv_file:
            file_lines = len(csv_file.readlines())
            csv_file.seek(0)

            ticket_values = []
            id_list = []
            ticket_list = []
            fields_dict = generate_fields_dict(csv_file)
            invalid_tickets = 1

            for i in range(file_lines - 1):
                current_line = csv_file.readline()
                while current_line.count(";") > 0:
                    ticket_values.append(current_line[:current_line.find(";")])
                    current_line = current_line[current_line.find(";") + 1:]
                if i >= 0:
                    if len(ticket_values) == len(fields_dict) - 2 \
                            and sum(ticket_values[0].isdigit() for _ in ticket_values[0]) == 6:
                        id_list.append(int(ticket_values[0]))
                        ticket_list.append(ticket_values[:])
                    else:
                        print("Ticket %s cannot be processed, as it contains %s semicolons when %s were expected." % (
                            ticket_values[0], len(ticket_values) - 2, len(fields_dict) - 2))
                        invalid_tickets += 1
                ticket_values *= 0
        edits_file_name = csv_file_name[:-4] + "_edits.txt"
        while True:
            overwritten_list = overwrite(ticket_list, id_list, fields_dict, edits_file_name)
            option = input("[edit], [display] or [exit]? ")
            print("")
            if option.lower() == "edit":
                edit_ticket(id_list, fields_dict, edits_file_name)
            elif option.lower() == "display":
                option = input("[console] or [excel]? ")
                if option.lower() == "console":
                    console_display(overwritten_list, fields_dict)
                elif option.lower() == "excel":
                    excel_display(overwritten_list, fields_dict, csv_file_name, fields_dict)
                else:
                    print("Invalid option")
            elif option.lower() == "exit":
                break
            else:
                print("Invalid option")
    elif csv_files_counter > 1:
        print("More than one CSV file present, please remove unused file(s)")
    else:
        print("Please put a CSV file in the programs directory")


main()
