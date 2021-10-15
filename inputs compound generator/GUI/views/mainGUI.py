import PySimpleGUI

class mainGUI:

    global list_configs
    list_configs = []

    def init_GUI(self):
        menu_def = [
            ['File', ['Settings', '---', 'Quit']],
            ['Data', ['Export DP']]
        ]

        layout_cabal = [
            [
                PySimpleGUI.Table(
                    values=[["-", "-", "-", "-"]],
                    headings=[
                        "Name",
                        "Population",
                        "Cabal ACA",
                        "Calc"
                    ],
                    auto_size_columns=False,
                    col_widths=[27, 15, 15, 15],
                    num_rows=30,
                    key="cabal_table",
                    expand_x=False
                ),
                PySimpleGUI.Column([
                    [PySimpleGUI.Text("--", key="cabal_response", font='TkFixedFont')],
                    [PySimpleGUI.Button("Plot", key="cabal_plot")]
                ])
            ],
        ]

        layout_dbo = [
            [
                PySimpleGUI.Table(
                    values=[["-", "-", "-", "-"]],
                    headings=[
                        "Name",
                        "Population",
                        "dbo ACA",
                        "Calc"
                    ],
                    auto_size_columns=False,
                    col_widths=[27, 15, 15, 15],
                    num_rows=30,
                    key="dbo_table",
                    expand_x=False
                ),
                PySimpleGUI.Column([
                    [PySimpleGUI.Text("--", key="dbo_response", font='TkFixedFont')],
                    [PySimpleGUI.Button("Plot", key="dbo_plot")]
                ])
            ],
        ]

        layout_ton = [
            [
                PySimpleGUI.Table(
                    values=[["-", "-", "-", "-"]],
                    headings=[
                        "Name",
                        "Population",
                        "ton ACA",
                        "Calc"
                    ],
                    auto_size_columns=False,
                    col_widths=[27, 15, 15, 15],
                    num_rows=30,
                    key="ton_table",
                    expand_x=False
                ),
                PySimpleGUI.Column([
                    [PySimpleGUI.Text("--", key="ton_response", font='TkFixedFont')],
                    [PySimpleGUI.Button("Plot", key="ton_plot")]
                ])
            ],
        ]

        layout_top = [
            [
                PySimpleGUI.Table(
                    values=[["-", "-", "-", "-"]],
                    headings=[
                        "Name",
                        "Population",
                        "top ACA",
                        "Calc"
                    ],
                    auto_size_columns=False,
                    col_widths=[27, 15, 15, 15],
                    num_rows=30,
                    key="top_table",
                    expand_x=False
                ),
                PySimpleGUI.Column([
                    [PySimpleGUI.Text("--", key="top_response", font='TkFixedFont')],
                    [PySimpleGUI.Button("Plot", key="top_plot")]
                ])
            ],
        ]

        tabs_calc_ratio = PySimpleGUI.TabGroup([[
            PySimpleGUI.Tab('Cabal', layout_cabal),
            PySimpleGUI.Tab('DBO', layout_dbo),
            PySimpleGUI.Tab('TON', layout_ton),
            PySimpleGUI.Tab('TOP', layout_top)
        ]], expand_x=True)

        # C1 Total aigua canalitzada (Utilitzada per calcular C4 i C6)
        # C2 Fosa septica
        # C3 Directe a medi !!!!! No és diferenciable de C6 !!!!
        # C4 Entrada canalitzada depuradora
        # C5 Entrada transportada depuradora
        # C6 Canalitzada i a medi
        layout = [
            [PySimpleGUI.Menu(menu_def, tearoff=False)],
            [PySimpleGUI.Text("SWAT+ Input Generator (TRAÇA)")],
            [PySimpleGUI.Text("Number of discharge points: ", key='dp_total')],
            [PySimpleGUI.Text("Number of industries: ", key='industries_total')],
            [PySimpleGUI.Table(
                values=[["-", "-", "-", "-"]],
                headings=[
                    "EDAR EU_ID",
                    "DP ID",
                    "Name",
                    "Latitude",
                    "Longitude",
                    "Population",
                    "Septic tank (C2)",
                    "Direct (C3/C6)",
                    "Canalized to WWTP (C4)",
                    "Transported to WWTP (C5)"
                ],
                auto_size_columns=False,
                col_widths=[17, 17, 27, 10, 15, 15, 15, 15, 15],
                num_rows=10,
                key="dp_table",
                expand_x=True
            )],
            [tabs_calc_ratio]

        ]

        return layout