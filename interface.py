import PySimpleGUI as sg

sg.theme("DarkTeal6")  # Add a touch of color

# All the stuff inside your window.
query_column = [
    [
        sg.Text("Query 1:"),
        sg.Multiline(size=(60, 10), key="-QUERY1-", enable_events=True,  background_color='white'),
    ],
    [
        sg.Text("Query 2:"),
        sg.Multiline(size=(60, 10), key="-QUERY2-", enable_events=True, background_color='white'),
    ],
]
queryplan_column = [
    [
        sg.Multiline(size=(60, 10), key="-QUERYPLAN1-", disabled=True, background_color='white'),
    ],
    [
        sg.Multiline(size=(60, 10), key="-QUERYPLAN2-", disabled=True, background_color='white'),
    ],
]
error_column = [
    [sg.Text(size=(75, 1), key="-ERROR-", text_color="red")],
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
    [sg.Column(error_column, vertical_alignment="center", justification="center")],
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
        error_msg = ""
        # Clear previous message
        window["-QUERYPLAN1-"].update("")
        window["-QUERYPLAN2-"].update("")

        if not values["-QUERY1-"]:
            window["-QUERY1-"].update(background_color='pink')
            error_msg += "Please enter a query in Query 1.\n"
        else:
            window["-QUERY1-"].update(background_color='white')

        if not values["-QUERY2-"]:
            window["-QUERY2-"].update(background_color='pink')
            error_msg += "Please enter a query in Query 2.\n"
        else:
            window["-QUERY2-"].update(background_color='white')

        if error_msg:
            window["-ERROR-"].update(error_msg)
        else:
            window["-QUERYPLAN1-"].update(f'Query 1: {values["-QUERY1-"]}')
            window["-QUERYPLAN2-"].update(f'Query 2: {values["-QUERY2-"]}')

            # Clear any existing error messages
            window["-ERROR-"].update("")

    elif event == "-QUERY1-" or event == "-QUERY2-":
        if values[event]:
            window[event].update(background_color="white")


window.close()