import PySimpleGUI as sg

# define the window layout
layout = [
    [
        sg.Multiline(
            "This is an example of highlighting text in PySimpleGUI.",
            key="-MULTILINE-",
            size=(40, 10),
        )
    ]
]

# create the window
window = sg.Window("Text Highlighting Example", layout, finalize=True)

# configure the tag for highlighting
window["-MULTILINE-"].Widget.tag_config("highlight", foreground="red")

# add the tag to the text to highlight
text = window["-MULTILINE-"].get()
start = text.find("example")
end = start + len("example")
window["-MULTILINE-"].Widget.tag_add("highlight", f"1.{start}", f"1.{end}")


# # add the tag to the text to highlight
# window["-MULTILINE-"].Widget.tag_add("highlight", "1.12", "1.19")  # highlight "example"

# event loop
while True:
    event, values = window.read()
    if event == sg.WIN_CLOSED:
        break

# close the window
window.close()
