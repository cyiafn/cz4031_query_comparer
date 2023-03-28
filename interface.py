import difflib
import PySimpleGUI as sg
from anytree import Node
from anytree.exporter import UniqueDotExporter
import sqlparse
from project import SQL_KEYWORDS, QueryPlan, QueryPlanNode, parseSQL, query
import os

os.environ["PATH"] += os.pathsep + "Graphviz/bin"  # Set Graphviz PATH

sg.theme("DarkTeal6")  # Add a touch of color

# All the stuff inside your window.
query_column = [
    [sg.Text("Query", font=("Helvitica", "16", "bold"))],
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
    [sg.Text("Query Plan Visualization", font=("Helvitica", "16", "bold"))],
    [
        sg.Column(queryplan1_image, scrollable=True, key="-QUERYPLAN1IMAGECOLUMN-"),
        sg.Button("View", key="-VIEW1-"),
    ],
    [
        sg.Column(queryplan2_image, scrollable=True, key="-QUERYPLAN2IMAGECOLUMN-"),
        sg.Button("View", key="-VIEW2-"),
    ],
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


explaination_column = [
    [sg.Text("Explaination", font=("Helvitica", "16", "bold"))],
    [
        sg.Multiline(
            size=(1620, 10),
            key="-EXPLAINATION-",
            disabled=True,
            background_color="white",
        )
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
    [
        sg.Column(
            explaination_column, vertical_alignment="center", justification="center"
        )
    ],
    [sg.Column(error_column, vertical_alignment="center", justification="center")],
    [sg.Column(button_column, vertical_alignment="center", justification="center")],
]


def build_tree_diff(root: QueryPlanNode, parent=None) -> Node:
    current_node = Node(root.node, parent=parent, color="red")
    if root.left:
        build_tree_diff(root.left, parent=current_node)
    if root.right:
        build_tree_diff(root.right, parent=current_node)

    return current_node


def build_tree(root: QueryPlanNode, parent=None, diff=None) -> Node:
    current_node = None
    if not diff:
        current_node = Node(root.node, parent=parent)
        if root.left:
            build_tree(root.left, parent=current_node)
        if root.right:
            build_tree(root.right, parent=current_node)
    else:
        if diff.node != root.node:
            current_node = Node(root.node, parent=parent, color="red")
            if root.left:
                build_tree_diff(root.left, parent=current_node)
            if root.right:
                build_tree_diff(root.right, parent=current_node)
        else:
            current_node = Node(root.node, parent=parent)
            if root.left:
                build_tree(root.left, parent=current_node, diff=diff)
            if root.right:
                build_tree(root.right, parent=current_node, diff=diff)

    return current_node


def set_name_color(node):
    attrs = []
    attrs += [f'label="{node.name}"'] if hasattr(node, "name") else []
    attrs += [f"color={node.color}"] if hasattr(node, "color") else []
    return ", ".join(attrs)


def get_diff(sql1, sql2):
    _, list1 = parseSQL(sql1)
    _, list2 = parseSQL(sql2)
    diff = []
    for i, (sublist1, sublist2) in enumerate(zip(list1, list2)):
        if sublist1 != sublist2:
            sm = difflib.SequenceMatcher(
                lambda x: x in SQL_KEYWORDS, sublist1, sublist2
            )
            for tag, i1, i2, j1, j2 in sm.get_opcodes():
                sublist1[0]
                print(tag)
                if tag == "replace":
                    diff.append(f'{sublist2[j1:j2][0].replace("/", "").lstrip()}')
                elif tag == "insert":
                    diff.append(f'{sublist2[j1:j2][0].replace("/", "").lstrip()}')

    return diff


def highlight_text(window: sg.Window, query_1, query_2):
    # Highlight the differences in query
    # start = query_1.find(diff_1)
    # end = start + len(diff_1)
    # window["-QUERY1-"].Widget.tag_add("highlight", f"1.{start}", f"1.{end}")
    diffs = get_diff(query_1, query_2)
    print(diffs)
    for diff in diffs:
        start = query_2.find(diff)
        if start != -1:
            end = start + len(diff)
            window["-QUERY2-"].Widget.tag_add("highlight", f"1.{start}", f"1.{end}")


def format_query(query):
    return sqlparse.format(query, keyword_case="upper", strip_comments=True)


def compare_btn(window: sg.Window, event, values):
    error_msg = ""

    if not values["-QUERY1-"] and not values["-QUERY2-"]:
        window["-QUERY1-"].update(background_color="pink")
        window["-QUERY2-"].update(background_color="pink")
        error_msg = "Please enter a query in Query 1 and Query 2."
    elif not values["-QUERY1-"]:
        window["-QUERY1-"].update(background_color="pink")
        error_msg = "Please enter a query in Query 1."
    elif not values["-QUERY2-"]:
        window["-QUERY2-"].update(background_color="pink")
        error_msg = "Please enter a query in Query 2."

    if error_msg:
        window["-ERROR-"].update(error_msg)
    else:
        # query_1 = window["-QUERY1-"].get()
        # query_2 = window["-QUERY2-"].get()

        query_1 = format_query(window["-QUERY1-"].get())
        query_2 = format_query(window["-QUERY2-"].get())
        window["-QUERY1-"].update(query_1)
        window["-QUERY2-"].update(query_2)

        q_plan_1 = query(query_1)
        q_plan_2 = query(query_2)

        q_plan_1_nodes = QueryPlan(q_plan_1["Plan"])
        q_plan_2_nodes = QueryPlan(q_plan_2["Plan"])

        output = q_plan_1_nodes.IsEqual(q_plan_2_nodes)
        equal = output[0]
        if not equal:
            tree_1 = build_tree(q_plan_1_nodes.root)
            tree_2 = build_tree(q_plan_2_nodes.root, diff=output[1])
        else:
            tree_1 = build_tree(q_plan_1_nodes.root)
            tree_2 = build_tree(q_plan_2_nodes.root)

        # Export tree to PNG file using UniqueDotExporter
        filename_1 = "tree_1.png"
        filename_2 = "tree_2.png"

        UniqueDotExporter(tree_1).to_picture(filename_1)
        window["-QUERYPLAN1IMAGE-"].update(filename_1)

        UniqueDotExporter(tree_2, nodeattrfunc=set_name_color).to_picture(filename_2)
        window["-QUERYPLAN2IMAGE-"].update(filename_2)

        highlight_text(window, query_1, query_2)

        # Refresh the update
        window.refresh()
        # Update for scroll area of Column element
        window["-QUERYPLAN1IMAGECOLUMN-"].contents_changed()
        window["-QUERYPLAN2IMAGECOLUMN-"].contents_changed()


def start_ui() -> None:
    # Create the Window
    window = sg.Window(
        "SQL Query Comparer", layout, size=(1620, 820), margins=(26, 26), finalize=True
    )

    # Add tag for highting the diff
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
            compare_btn(window, event, values)
        elif event == "-VIEW1-":
            sg.popup(
                keep_on_top=True,
                image="tree_1.png",
            )
        elif event == "-VIEW2-":
            sg.popup(
                keep_on_top=True,
                image="tree_2.png",
            )
        elif event == "-QUERY1-":
            error_msg = ""
            if values[event] and not values["-QUERY2-"]:
                window[event].update(background_color="white")
                error_msg = "Please enter a query in Query 2."
            elif values[event] and values["-QUERY2-"]:
                window[event].update(background_color="white")

            if window["-ERROR-"].get():
                window["-ERROR-"].update(error_msg)

            window[event].Widget.tag_remove("highlight", "1.0", "end")
        elif event == "-QUERY2-":
            error_msg = ""
            if values[event] and not values["-QUERY1-"]:
                window[event].update(background_color="white")
                error_msg = "Please enter a query in Query 1."
            elif values[event] and values["-QUERY1-"]:
                window[event].update(background_color="white")

            if window["-ERROR-"].get():
                window["-ERROR-"].update(error_msg)

            window[event].Widget.tag_remove("highlight", "1.0", "end")

    window.close()


start_ui()
