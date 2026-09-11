"""Extraction catalog and explicit parent relationships; not a PVA replacement."""

from layouts_icms_ipi import LAYOUTS_ICMS_IPI, NUMERIC_COLUMNS_ICMS_IPI, GROUPS_ICMS_IPI
from layouts_contribuicoes import (
    LAYOUTS_CONTRIBUICOES,
    NUMERIC_COLUMNS_CONTRIBUICOES,
    GROUPS_CONTRIBUICOES,
)

TYPES = {"ICMS_IPI": "EFD ICMS/IPI", "CONTRIBUICOES": "EFD Contribuições"}
DOCUMENTS = {
    "A100": "VL_DOC",
    "C100": "VL_DOC",
    "C395": "VL_DOC",
    "C500": "VL_DOC",
    "D100": "VL_DOC",
    "D500": "VL_DOC",
    "D700": "VL_DOC",
    "C800": "VL_CFE",
    "F100": "VL_OPER",
}
CANCELLED = {"02", "03", "04", "05"}


def get_layout_config(kind):
    if kind == "ICMS_IPI":
        return LAYOUTS_ICMS_IPI, NUMERIC_COLUMNS_ICMS_IPI, GROUPS_ICMS_IPI
    if kind == "CONTRIBUICOES":
        return LAYOUTS_CONTRIBUICOES, NUMERIC_COLUMNS_CONTRIBUICOES, GROUPS_CONTRIBUICOES
    raise ValueError("Tipo de EFD não reconhecido.")


def numeric_fields(code, fields, kind):
    explicit = get_layout_config(kind)[1].get(code, [])
    prefixes = ("VL_", "ALIQ_", "QTD", "QTDE", "QUANT_", "BC_", "SLD_", "REC_BRU_", "PESO_")
    return {f for f in fields if f in explicit or f.startswith(prefixes)}


def parents_for(kind):
    layouts, _, groups = get_layout_config(kind)
    parents = {}
    for root, children, _, _, header in groups.values():
        if header and root != header:
            parents[root] = header
        for child in children:
            parents[child] = root
    specific = {
        "0175": "0150",
        "0205": "0200",
        "0206": "0200",
        "0220": "0200",
        "C111": "C110",
        "C141": "C140",
        "D161": "D160",
        "D162": "D160",
        "E112": "E111",
        "E113": "E111",
        "E230": "E220",
        "E240": "E220",
        "E312": "E311",
        "E313": "E311",
        "G126": "G125",
        "G130": "G125",
        "G140": "G130",
        "H020": "H010",
        "H030": "H010",
        "K215": "K210",
        "K235": "K230",
        "K255": "K250",
        "K265": "K260",
        "K275": "K270",
        "K291": "K290",
        "K292": "K290",
        "K301": "K300",
        "K302": "K300",
        "A111": "A110",
        "M115": "M110",
        "M205": "M200",
        "M210": "M200",
        "M211": "M210",
        "M215": "M210",
        "M220": "M210",
        "M225": "M220",
        "M230": "M210",
        "M410": "M400",
        "M605": "M600",
        "M610": "M600",
        "M611": "M610",
        "M615": "M610",
        "M620": "M610",
        "M630": "M610",
        "M810": "M800",
        "C396": "C395",
        "D731": "D730",
        "0145": "0140",
    }
    if kind == "ICMS_IPI":
        specific.update(
            {
                c: "C170"
                for c in [
                    "C171",
                    "C172",
                    "C173",
                    "C174",
                    "C175",
                    "C176",
                    "C177",
                    "C178",
                    "C179",
                    "C180",
                    "C181",
                ]
            }
        )
        specific.update(
            {
                "C191": "C190",
                "C197": "C195",
                "D197": "D195",
                "C186": "C100",
                "0210": "0200",
                "0221": "0200",
            }
        )
    else:
        specific.update(
            {"0150": "0140", "0190": "0140", "0200": "0140", "0400": "0140", "0450": "0140"}
        )
    for child, parent in specific.items():
        if child in layouts and parent in layouts:
            parents[child] = parent
    return parents
