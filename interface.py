from anytree import Node
import PySimpleGUI as sg
from anytree.exporter import UniqueDotExporter
from project import QueryPlan, query
import os

os.environ["PATH"] += os.pathsep + "Graphviz/bin"  # Set Graphviz PATH

sg.theme("DarkTeal6")  # Add a touch of color

# Todo: Process the plan tree into a simple tree
# Add one more text box for explaination

# All the stuff inside your window.
query_column = [
    [
        sg.Text("Query 1:"),
        sg.Multiline(
            size=(60, 10), key="-QUERY1-", enable_events=True, background_color="white"
        ),
    ],
    [
        sg.Text("Query 2:"),
        sg.Multiline(
            size=(60, 10), key="-QUERY2-", enable_events=True, background_color="white"
        ),
    ],
]

queryplan1_image = [[sg.Image(size=(400, 300), key="-QUERYPLAN1IMAGE-")]]
queryplan2_image = [[sg.Image(size=(400, 300), key="-QUERYPLAN2IMAGE-")]]
queryplanimage_column = [
    [sg.Column(queryplan1_image, scrollable=True, key="-QUERYPLAN1IMAGECOLUMN-")],
    [sg.Column(queryplan2_image, scrollable=True, key="-QUERYPLAN2IMAGECOLUMN-")],
]

queryplan_column = [
    [
        sg.Multiline(
            size=(60, 10), key="-QUERYPLAN1-", disabled=True, background_color="white"
        ),
    ],
    [
        sg.Multiline(
            size=(60, 10), key="-QUERYPLAN2-", disabled=True, background_color="white"
        ),
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
        # sg.VSeperator(),
        # sg.Column(queryplan_column),
        sg.VSeperator(),
        sg.Column(queryplanimage_column),
    ],
    [sg.Column(error_column, vertical_alignment="center", justification="center")],
    [sg.Column(button_column, vertical_alignment="center", justification="center")],
]


def build_tree(node, parent=None):
    node_type = node["Node Type"]
    current_node = Node(node_type, parent=parent)
    if "Plans" in node:
        for child in node["Plans"]:
            build_tree(child, parent=current_node)
    return current_node


def compare_btn(window: sg.Window, event, values):
    error_msg = ""

    if not values["-QUERY1-"]:
        window["-QUERY1-"].update(background_color="pink")
        error_msg += "Please enter a query in Query 1.\n"
    else:
        window["-QUERY1-"].update(background_color="white")

    if not values["-QUERY2-"]:
        window["-QUERY2-"].update(background_color="pink")
        error_msg += "Please enter a query in Query 2.\n"
    else:
        window["-QUERY2-"].update(background_color="white")

    if error_msg:
        window["-ERROR-"].update(error_msg, background_color="white")
    else:
        q_plan_1 = query(values["-QUERY1-"])
        q_plan_2 = query(values["-QUERY2-"])

        tree_1 = build_tree(q_plan_1["Plan"])
        tree_2 = build_tree(q_plan_2["Plan"])

        # Export tree to PNG file using UniqueDotExporter
        filename_1 = "tree_1.png"
        filename_2 = "tree_2.png"
        dot_exporter = UniqueDotExporter(tree_1)
        dot_exporter.to_picture(filename_1)
        window["-QUERYPLAN1IMAGE-"].update(filename_1)
        dot_exporter = UniqueDotExporter(tree_2)
        dot_exporter.to_picture(filename_2)
        window["-QUERYPLAN2IMAGE-"].update(filename_2)

        # Refresh the update
        window.refresh()
        # Update for scroll area of Column element
        window["-QUERYPLAN1IMAGECOLUMN-"].contents_changed()
        window["-QUERYPLAN2IMAGECOLUMN-"].contents_changed()

        text = window["-QUERY1-"].get()
        start = text.find("select")
        end = start + len("select")
        window["-QUERY1-"].Widget.tag_add("highlight", f"1.{start}", f"1.{end}")

        text = window["-QUERY2-"].get()
        start = text.find("n_name")
        end = start + len("n_name")
        window["-QUERY2-"].Widget.tag_add("highlight", f"1.{start}", f"1.{end}")

        # Clear any existing error messages
        window["-ERROR-"].update("")


def start_ui() -> None:
    # Create the Window
    window = sg.Window(
        "SQL Query Comparer", layout, size=(1620, 820), margins=(26, 26), finalize=True
    )

    window["-QUERY1-"].Widget.tag_config("highlight", foreground="red")
    window["-QUERY2-"].Widget.tag_config("highlight", foreground="red")

    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read()

        if (
            event == sg.WIN_CLOSED or event == "-QUIT-"
        ):  # if user closes window or clicks cancel
            break
        elif event == "-COMPARE-":
            error_msg = ""
            # # Clear previous message
            # window["-QUERYPLAN1-"].update("")
            # window["-QUERYPLAN2-"].update("")

            if not values["-QUERY1-"]:
                window["-QUERY1-"].update(background_color="pink")
                error_msg += "Please enter a query in Query 1.\n"
            else:
                window["-QUERY1-"].update(background_color="white")

            if not values["-QUERY2-"]:
                window["-QUERY2-"].update(background_color="pink")
                error_msg += "Please enter a query in Query 2.\n"
            else:
                window["-QUERY2-"].update(background_color="white")

            if error_msg:
                window["-ERROR-"].update(error_msg, background_color="white")
            else:
                q_plan_1 = query(values["-QUERY1-"])
                q_plan_2 = query(values["-QUERY2-"])

                tree_1 = build_tree(q_plan_1["Plan"])
                tree_2 = build_tree(q_plan_2["Plan"])

                # Export tree to PNG file using UniqueDotExporter
                filename_1 = "tree_1.png"
                filename_2 = "tree_2.png"
                dot_exporter = UniqueDotExporter(tree_1)
                dot_exporter.to_picture(filename_1)
                window["-QUERYPLAN1IMAGE-"].update(filename_1)
                dot_exporter = UniqueDotExporter(tree_2)
                dot_exporter.to_picture(filename_2)
                window["-QUERYPLAN2IMAGE-"].update(filename_2)

                # Refresh the update
                window.refresh()
                # Update for scroll area of Column element
                window["-QUERYPLAN1IMAGECOLUMN-"].contents_changed()
                window["-QUERYPLAN2IMAGECOLUMN-"].contents_changed()

                text = window["-QUERY1-"].get()
                start = text.find("select")
                end = start + len("select")
                window["-QUERY1-"].Widget.tag_add("highlight", f"1.{start}", f"1.{end}")

                text = window["-QUERY2-"].get()
                start = text.find("n_name")
                end = start + len("n_name")
                window["-QUERY2-"].Widget.tag_add("highlight", f"1.{start}", f"1.{end}")

                # Clear any existing error messages
                window["-ERROR-"].update("")

        elif event == "-QUERY1-" or event == "-QUERY2-":
            if values[event]:
                window[event].update(background_color="white")

    window.close()


start_ui()
