"""Small fictitious, locally generated sample for onboarding and regression tests."""

from schema import get_layout_config


def record(kind, code, **values):
    return (
        "|"
        + "|".join(
            code if f == "REG" else str(values.get(f, "")) for f in get_layout_config(kind)[0][code]
        )
        + "|\n"
    )


def sample_file(kind="ICMS_IPI", count=6):
    cnpj = "11222333000181"
    lines = [
        record(
            kind,
            "0000",
            COD_VER="020" if kind == "ICMS_IPI" else "006",
            COD_FIN="0",
            TIPO_ESCRIT="0",
            DT_INI="01012026",
            DT_FIN="31012026",
            NOME="EMPRESA DEMONSTRACAO",
            CNPJ=cnpj,
            UF="SP",
            IND_PERFIL="A",
            IND_ATIV="1",
        )
    ]
    if kind == "CONTRIBUICOES":
        lines.append(
            record(kind, "0140", COD_EST="01", NOME="EMPRESA DEMONSTRACAO", CNPJ=cnpj, UF="SP")
        )
    lines.append(
        record(
            kind, "0150", COD_PART="P1", NOME="Fornecedor demonstrativo", COD_PAIS="1058", CNPJ=cnpj
        )
    )
    lines.append(record(kind, "C001", IND_MOV="0"))
    if kind == "CONTRIBUICOES":
        lines.append(record(kind, "C010", CNPJ=cnpj, IND_ESCRI="1"))
    for i in range(count):
        amount = 100 * (i + 1)
        oper = str(i % 2)
        lines.append(
            record(
                kind,
                "C100",
                IND_OPER=oper,
                IND_EMIT="1",
                COD_PART="P1",
                COD_MOD="55",
                COD_SIT="00",
                SER="1",
                NUM_DOC=str(1000 + i),
                DT_DOC=f"{10 + i:02}012026",
                VL_DOC=f"{amount},00",
                VL_MERC=f"{amount},00",
                VL_ICMS=f"{amount * 18 // 100},00",
                VL_PIS="1,65",
                VL_COFINS="7,60",
            )
        )
        for item in (1, 2):
            lines.append(
                record(
                    kind,
                    "C170",
                    NUM_ITEM=str(item),
                    COD_ITEM=f"ITEM{item}",
                    QTD="1,000",
                    VL_ITEM=f"{amount // 2},00",
                    CFOP="1102" if oper == "0" else "5102",
                )
            )
        if kind == "ICMS_IPI":
            for cfop in ("1102", "2102") if oper == "0" else ("5102", "6102"):
                lines.append(
                    record(
                        kind,
                        "C190",
                        CST_ICMS="000",
                        CFOP=cfop,
                        ALIQ_ICMS="18,00",
                        VL_OPR=f"{amount // 2},00",
                    )
                )
    block_count = sum(line.startswith("|C") for line in lines) + 1
    lines.append(record(kind, "C990", QTD_LIN_C=str(block_count)))
    lines.append(record(kind, "9999", QTD_LIN=str(len(lines) + 1)))
    return "".join(lines).encode("utf-8")
