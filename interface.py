import PySimpleGUI as sg

sg.theme("DarkTeal6")  # Add a touch of color

# All the stuff inside your window.
query_column = [
    [
        sg.Text("Query 1:"),
        sg.Multiline(size=(60, 10), key="-QUERY1-"),
    ],
    [
        sg.Text("Query 2:"),
        sg.Multiline(size=(60, 10), key="-QUERY2-"),
    ],
]
queryplan_column = [
    [
        sg.Multiline(size=(60, 10), key="-QUERYPLAN1-", disabled=True),
    ],
    [
        sg.Multiline(size=(60, 10), key="-QUERYPLAN2-", disabled=True),
    ],
]
button_column = [
    [sg.Button("Compare", key="-COMPARE-"), sg.Button("Quit", key="-QUIT-")],
]
layout = [
    [
        sg.Column(query_column),
        sg.VSeperator(),
        sg.Column(queryplan_column),
    ],
    [sg.Column(button_column, vertical_alignment="center", justification="center")],
]

# Create the Window
window = sg.Window("SQL Query Comparer", layout, margins=(26, 26))

# Event Loop to process "events" and get the "values" of the inputs
while True:
    event, values = window.read()

    if (
        event == sg.WIN_CLOSED or event == "-QUIT-"
    ):  # if user closes window or clicks cancel
        break
    elif event == "-COMPARE-":
        # Check if the text box is empty
        if values["-QUERY1-"] == "" or values["-QUERY2-"] == "":
            sg.popup("Either Query 1 or Query 2 is empty!", title="Error Message")
            continue

        window["-QUERYPLAN1-"].update(f'Query 1: {values["-QUERY1-"]}')
        window["-QUERYPLAN2-"].update(f'Query 2: {values["-QUERY2-"]}')

window.close()
