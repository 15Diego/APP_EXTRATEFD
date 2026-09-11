"""SPED extraction with provenance, quarantine and non-multiplying tables."""

from __future__ import annotations
import hashlib
import base64
import json
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import pandas as pd
import yaml
from schema import DOCUMENTS, CANCELLED, get_layout_config, numeric_fields, parents_for
from validators import validate_cnpj

CONFIG = yaml.safe_load(Path(__file__).with_name("config.yaml").read_text(encoding="utf-8"))


def get_config(path, default=None):
    value = CONFIG
    for key in path.split("."):
        value = value.get(key, {}) if isinstance(value, dict) else {}
    return default if value == {} else value


def parse_sped_line(line):
    text = line.rstrip("\r\n").lstrip("\ufeff")
    if not text.startswith("|") or not text.endswith("|"):
        raise ValueError("A linha deve começar e terminar com |.")
    return text[1:-1].split("|")


def parse_date(value):
    if not re.fullmatch(r"\d{8}", value):
        raise ValueError("Data deve usar DDMMAAAA.")
    return datetime.strptime(value, "%d%m%Y").date()


def parse_number(value):
    if not value.strip():
        return None
    if not re.fullmatch(r"-?(?:\d+|\d{1,3}(?:\.\d{3})+)(?:,\d+)?", value):
        raise ValueError("Número inválido; use vírgula como separador decimal.")
    return Decimal(value.replace(".", "").replace(",", "."))


def decode_content(content, encoding="auto"):
    if encoding != "auto":
        return content.decode(encoding, errors="strict"), encoding
    try:
        return content.decode("utf-8-sig"), "utf-8-sig"
    except UnicodeDecodeError:
        try:
            return content.decode("cp1252"), "cp1252"
        except UnicodeDecodeError:
            return content.decode("latin-1"), "latin-1"


def detect_efd_type(content):
    if isinstance(content, bytes):
        content, _ = decode_content(content)
    fields = parse_sped_line(next((s for s in content.splitlines() if s.strip()), ""))
    if not fields or fields[0] != "0000":
        raise ValueError("O primeiro registro deve ser 0000.")
    candidates = []
    for kind in ("ICMS_IPI", "CONTRIBUICOES"):
        layout = get_layout_config(kind)[0]["0000"]
        if len(fields) < len(layout):
            continue
        row = dict(zip(layout, fields))
        try:
            if parse_date(row["DT_INI"]) <= parse_date(row["DT_FIN"]) and row["CNPJ"]:
                candidates.append(kind)
        except ValueError:
            pass
    if len(candidates) != 1:
        raise ValueError("Cabeçalho 0000 inválido ou ambíguo. Confira tipo e período do arquivo.")
    return candidates[0]


@dataclass
class FileResult:
    metadata: dict
    tables: dict = field(default_factory=dict)
    raw_tables: dict = field(default_factory=dict)
    issues: list = field(default_factory=list)
    documents: pd.DataFrame = field(default_factory=pd.DataFrame)


@dataclass
class BatchResult:
    files: list = field(default_factory=list)
    failures: list = field(default_factory=list)
    rejected_files: list = field(default_factory=list)

    @property
    def documents(self):
        frames = [f.documents for f in self.files if not f.documents.empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    @property
    def issues(self):
        return pd.DataFrame([i for f in self.files for i in f.issues] + self.failures)

    @property
    def manifest(self):
        return pd.DataFrame([f.metadata for f in self.files])


def issue(name, line, code, level, message, field_name=""):
    return {
        "ARQUIVO": name,
        "LINHA": line,
        "REGISTRO": code,
        "NIVEL": level,
        "CAMPO": field_name,
        "MENSAGEM": message,
    }


def validate_row(code, row, numeric, kind):
    problems = []
    required = {
        "0000": ["COD_VER", "CNPJ", "DT_INI", "DT_FIN", "NOME"],
        "C100": ["IND_OPER", "IND_EMIT", "COD_MOD", "COD_SIT", "NUM_DOC"],
        "D100": ["IND_OPER", "IND_EMIT", "COD_MOD", "COD_SIT", "NUM_DOC"],
        "A100": ["IND_OPER", "IND_EMIT", "COD_SIT", "NUM_DOC"],
        "C170": ["NUM_ITEM", "COD_ITEM", "QTD", "VL_ITEM"],
        "A170": ["NUM_ITEM", "COD_ITEM", "VL_ITEM"],
        "F100": ["IND_OPER", "DT_OPER"],
    }
    for key in required.get(code, []):
        if key in row and not row[key]:
            problems.append((key, "Campo obrigatório vazio."))
    if code in DOCUMENTS and row.get("COD_SIT") not in CANCELLED:
        for key in (DOCUMENTS[code], "DT_OPER" if code == "F100" else "DT_DOC"):
            if key in row and not row[key]:
                problems.append((key, "Valor ou data do documento/operação vazio."))
    for key, value in row.items():
        if not value:
            continue
        try:
            if key.startswith("DT_"):
                parse_date(value)
            elif key in numeric:
                parse_number(value)
            elif key == "IND_OPER" and value not in ("0", "1", "2"):
                raise ValueError("Indicador de operação inválido.")
            elif key == "CFOP" and not re.fullmatch(r"[123567]\d{3}", value):
                raise ValueError("Formato de CFOP inválido.")
            elif key in ("CHV_NFE", "CHV_CTE", "CHV_CFE", "CHV_DOCe") and not re.fullmatch(
                r"\d{44}", value
            ):
                raise ValueError("Chave deve conter 44 dígitos.")
            elif (
                (key == "CNPJ" or key.startswith("CNPJ_"))
                and "CPF" not in key
                and not validate_cnpj(value)
            ):
                raise ValueError("CNPJ inválido ou formato não reconhecido.")
        except (ValueError, TypeError) as exc:
            problems.append((key, str(exc)))
    return problems


def process_content(content, name, expected_type="auto", strict=False, encoding="auto"):
    started = time.perf_counter()
    name = Path(name).name
    maximum = get_config("processing.max_file_size_mb", 100) * 1024 * 1024
    if not content or len(content) > maximum:
        raise ValueError(f"Arquivo vazio ou maior que {maximum // 1024 // 1024} MB.")
    text, codec = decode_content(content, encoding)
    kind = detect_efd_type(text)
    if expected_type != "auto" and expected_type != kind:
        raise ValueError("O tipo selecionado não corresponde ao cabeçalho do arquivo.")
    layouts, _, _ = get_layout_config(kind)
    parents = parents_for(kind)
    digest = hashlib.sha256(content).hexdigest()
    physical_lines = text.splitlines()
    max_lines = get_config("processing.max_lines", 500000)
    if len(physical_lines) > max_lines:
        raise ValueError(f"Limite de {max_lines:,} linhas excedido; divida o processamento.")
    first_line = next(i for i, s in enumerate(physical_lines, 1) if s.strip())
    header = dict(zip(layouts["0000"], parse_sped_line(physical_lines[first_line - 1])))
    problems = validate_row("0000", header, set(), kind)
    if problems:
        raise ValueError("Cabeçalho inválido: " + "; ".join(f"{k}: {m}" for k, m in problems))
    metadata = {
        "ARQUIVO": name,
        "ARQUIVO_ID": digest,
        "TIPO_EFD": kind,
        "EMPRESA": header["NOME"],
        "CNPJ_EMPRESA": header["CNPJ"],
        "PERIODO_INICIO": header["DT_INI"],
        "PERIODO_FIM": header["DT_FIN"],
        "VERSAO_LAYOUT": header["COD_VER"],
        "RETIFICADORA": header.get("TIPO_ESCRIT", header.get("COD_FIN", "")) == "1",
        "ENCODING": codec,
        "TAMANHO_BYTES": len(content),
    }
    result = FileResult(metadata)
    rows, originals = defaultdict(list), defaultdict(list)
    active, counts = {}, Counter()
    current_est, previous_block = header["CNPJ"], None
    declared_counts = []
    for number, raw in enumerate(physical_lines, 1):
        counts["TOTAL_LINHAS"] += 1
        code = raw.lstrip("\ufeff")[1:5] if raw.lstrip("\ufeff").startswith("|") else "INVALIDO"
        row_id = f"{digest}:{number}"
        origin = {
            "ARQUIVO": name,
            "ARQUIVO_ID": digest,
            "LINHA": number,
            "REGISTRO_ID": row_id,
            "TIPO_EFD": kind,
            "CNPJ_EMPRESA": header["CNPJ"],
            "CNPJ_ESTABELECIMENTO": current_est,
        }
        if not raw.strip():
            counts["IGNORADAS"] += 1
            originals["IGNORADAS"].append(
                {**origin, "CONTEUDO_ORIGINAL": raw, "STATUS": "Ignorado"}
            )
            continue
        try:
            parts = parse_sped_line(raw)
            code = parts[0]
            if not re.fullmatch(r"[A-Z0-9]{4}", code):
                raise ValueError("Código de registro inválido.")
        except ValueError as exc:
            result.issues.append(issue(name, number, code, "Erro", str(exc)))
            originals["REJEITADOS"].append(
                {**origin, "CONTEUDO_ORIGINAL": raw, "STATUS": "Rejeitado"}
            )
            counts["REJEITADAS"] += 1
            active.clear()
            continue
        block = code[0]
        if block != previous_block:
            active.clear()
            current_est = header["CNPJ"]
        previous_block = block
        origin["CNPJ_ESTABELECIMENTO"] = current_est
        if code.endswith(("001", "990")) or code in ("0000", "9999"):
            active.clear()
        if code not in layouts:
            counts["NAO_SUPORTADAS"] += 1
            originals[code].append(
                {
                    **origin,
                    **{f"CAMPO_{i:03}": v for i, v in enumerate(parts)},
                    "CONTEUDO_ORIGINAL": raw,
                    "STATUS": "Não suportado",
                }
            )
            result.issues.append(
                issue(
                    name,
                    number,
                    code,
                    "Aviso",
                    "Registro preservado sem interpretação; fora dos indicadores.",
                )
            )
            active.clear()
            continue
        # Keep only the current ancestor path. A later sibling closes the previous
        # sibling's scope; its children must never attach backwards to an old row.
        allowed = {f"{block}001"}
        ancestor = parents.get(code)
        while ancestor:
            allowed.add(ancestor)
            ancestor = parents.get(ancestor)
        active = {key: value for key, value in active.items() if key in allowed}
        fields = layouts[code]
        values = dict(zip(fields, parts))
        values.update({f"EXTRA_{i:03}": v for i, v in enumerate(parts[len(fields) :], 1)})
        numeric = numeric_fields(code, fields, kind)
        errors = validate_row(code, values, numeric, kind)
        if len(parts) != len(fields):
            errors.append(
                (
                    "ESTRUTURA",
                    f"{len(parts)} campos recebidos; catálogo espera {len(fields)}. Conteúdo preservado; interpretação suspensa.",
                )
            )
        if code == "0000" and number != first_line:
            errors.append(("ESTRUTURA", "Mais de um cabeçalho 0000 no mesmo arquivo."))
        parent_row = active.get(parents.get(code))
        if code in parents and parent_row is None:
            errors.append(
                ("VINCULO", f"Registro pai {parents[code]} ausente ou rejeitado neste contexto.")
            )
        if code in ("0140", "A010", "C010", "D010", "F010") and not errors:
            current_est = values.get("CNPJ") or header["CNPJ"]
        origin["CNPJ_ESTABELECIMENTO"] = current_est
        originals[code].append(
            {
                **values,
                **origin,
                "CONTEUDO_ORIGINAL": raw,
                "STATUS": "Rejeitado" if errors else "Aceito",
            }
        )
        if errors:
            counts["REJEITADAS"] += 1
            for key, message in errors:
                result.issues.append(issue(name, number, code, "Erro", message, key))
            continue
        row = {**values, **origin, "PAI_ID": parent_row["REGISTRO_ID"] if parent_row else ""}
        row["DOCUMENTO_ID"] = (
            row_id
            if code in DOCUMENTS
            else (parent_row.get("DOCUMENTO_ID", "") if parent_row else "")
        )
        for key in numeric:
            row[key] = parse_number(values.get(key, ""))
        for key in fields:
            if key.startswith("DT_") and values.get(key):
                row[key] = parse_date(values[key])
        active[code] = row
        rows[code].append(row)
        counts["ACEITAS"] += 1
        if code == "9999":
            declared_counts.append((number, values.get("QTD_LIN", "")))
    if not declared_counts:
        result.issues.append(issue(name, 0, "9999", "Erro", "Registro de encerramento ausente."))
    for number, declared in declared_counts:
        if not declared.isdigit() or int(declared) != len([s for s in physical_lines if s.strip()]):
            result.issues.append(
                issue(
                    name,
                    number,
                    "9999",
                    "Erro",
                    "Contagem declarada difere das linhas não vazias do arquivo.",
                )
            )
    if next((s for s in reversed(physical_lines) if s.strip()), "").split("|")[1:2] != ["9999"]:
        result.issues.append(issue(name, 0, "9999", "Erro", "O último registro deve ser 9999."))
    result.tables = {k: pd.DataFrame(v) for k, v in rows.items()}
    result.raw_tables = {k: pd.DataFrame(v) for k, v in originals.items()}
    result.documents = build_documents(result)
    check_cross_totals(result)
    for entry in result.issues:
        entry["ARQUIVO_ID"] = digest
    metadata.update(
        {
            key: counts[key]
            for key in ("TOTAL_LINHAS", "ACEITAS", "REJEITADAS", "NAO_SUPORTADAS", "IGNORADAS")
        }
    )
    metadata["APROVEITAMENTO_PCT"] = round(
        100 * counts["ACEITAS"] / max(1, counts["TOTAL_LINHAS"] - counts["IGNORADAS"]), 2
    )
    metadata["OCORRENCIAS"] = len(result.issues)
    metadata["TEMPO_S"] = round(time.perf_counter() - started, 3)
    metadata["STATUS"] = (
        "Com erros"
        if any(i["NIVEL"] == "Erro" for i in result.issues)
        else ("Com avisos" if result.issues else "Concluído")
    )
    if strict and result.issues:
        raise ValueError(
            f"Modo estrito: {len(result.issues)} ocorrência(s); primeiro motivo: {result.issues[0]['MENSAGEM']}"
        )
    return result


def build_documents(result):
    participants = {}
    for row in result.tables.get("0150", pd.DataFrame()).to_dict("records"):
        key = (row["CNPJ_ESTABELECIMENTO"], row["COD_PART"])
        participants[key] = None if key in participants else row
    children = defaultdict(set)
    for table in result.tables.values():
        if "CFOP" in table:
            for row in table.to_dict("records"):
                if row.get("DOCUMENTO_ID") and row.get("CFOP"):
                    children[row["DOCUMENTO_ID"]].add(row["CFOP"])
    documents = []
    for code, amount in DOCUMENTS.items():
        for row in result.tables.get(code, pd.DataFrame()).to_dict("records"):
            participant = participants.get((row["CNPJ_ESTABELECIMENTO"], row.get("COD_PART", "")))
            if row.get("COD_PART") and not participant:
                result.issues.append(
                    issue(
                        row["ARQUIVO"],
                        row["LINHA"],
                        code,
                        "Aviso",
                        "Participante ausente ou ambíguo no cadastro 0150.",
                        "COD_PART",
                    )
                )
            participant = participant or {}
            documents.append(
                {
                    **row,
                    "REGISTRO": code,
                    "DATA": row.get("DT_DOC", row.get("DT_OPER")),
                    "VALOR": row.get(amount),
                    "PARTICIPANTE": participant.get("NOME", row.get("COD_PART", "")),
                    "CNPJ_PARTICIPANTE": participant.get("CNPJ", ""),
                    "PARTICIPANTE_ID": participant.get("CNPJ")
                    or f"{row['ARQUIVO_ID']}:{row.get('COD_PART', '')}",
                    "OPERACAO": {"0": "Entrada", "1": "Saída", "2": "Outra"}.get(
                        row.get("IND_OPER", ""), "Não informada"
                    ),
                    "CFOPS": ", ".join(sorted(children[row["DOCUMENTO_ID"]])),
                    "CANCELADO": row.get("COD_SIT", "") in CANCELLED,
                }
            )
    return pd.DataFrame(documents)


def check_cross_totals(result):
    parents = result.tables.get("C100", pd.DataFrame())
    children = result.tables.get("C170", pd.DataFrame())
    if parents.empty or children.empty:
        return
    totals = children.groupby("DOCUMENTO_ID")["VL_ITEM"].agg(
        lambda s: sum((v for v in s if isinstance(v, Decimal)), Decimal(0))
    )
    tolerance = Decimal(str(get_config("processing.validation_tolerance", 0.01)))
    for row in parents.to_dict("records"):
        expected = row.get("VL_MERC")
        if (
            isinstance(expected, Decimal)
            and row["DOCUMENTO_ID"] in totals
            and row.get("COD_SIT") not in CANCELLED
        ):
            difference = expected - totals[row["DOCUMENTO_ID"]]
            if abs(difference) > tolerance:
                result.issues.append(
                    issue(
                        row["ARQUIVO"],
                        row["LINHA"],
                        "C100",
                        "Aviso",
                        f"VL_MERC difere da soma dos itens C170 em {difference}. Confira descontos e escrituração.",
                        "VL_MERC",
                    )
                )


def process_batch(files, expected_type="auto", strict=False, encoding="auto", progress=None):
    if len(files) > get_config("processing.max_files", 20):
        raise ValueError("Quantidade de arquivos acima do limite configurado.")
    if (
        sum(len(data) for _, data in files)
        > get_config("processing.max_batch_size_mb", 200) * 1024 * 1024
    ):
        raise ValueError("O lote excede o limite total configurado.")
    result, seen = BatchResult(), set()
    for index, (name, content) in enumerate(files):
        digest = hashlib.sha256(content).hexdigest()
        try:
            if digest in seen:
                result.failures.append(
                    issue(
                        name,
                        0,
                        "ARQUIVO",
                        "Aviso",
                        "Arquivo idêntico já recebido; duplicata não adicionada.",
                    )
                )
                continue
            seen.add(digest)
            result.files.append(process_content(content, name, expected_type, strict, encoding))
        except (ValueError, UnicodeError) as exc:
            result.failures.append(issue(name, 0, "ARQUIVO", "Erro", str(exc)))
            result.rejected_files.append(
                {
                    "ARQUIVO": name,
                    "ARQUIVO_ID": digest,
                    "MOTIVO": str(exc),
                    "CONTEUDO_BASE64": base64.b64encode(content).decode("ascii"),
                }
            )
        finally:
            if progress:
                progress((index + 1) / len(files), name)
    return result


def conflicting_files(batch, file_ids=None):
    files = [
        f.metadata for f in batch.files if file_ids is None or f.metadata["ARQUIVO_ID"] in file_ids
    ]
    conflicts = []
    for index, a in enumerate(files):
        for b in files[index + 1 :]:
            if a["TIPO_EFD"] == b["TIPO_EFD"] and a["CNPJ_EMPRESA"] == b["CNPJ_EMPRESA"]:
                if max(parse_date(a["PERIODO_INICIO"]), parse_date(b["PERIODO_INICIO"])) <= min(
                    parse_date(a["PERIODO_FIM"]), parse_date(b["PERIODO_FIM"])
                ):
                    conflicts.append((a["ARQUIVO"], b["ARQUIVO"]))
    return conflicts


def filter_documents(batch, filters):
    df = batch.documents
    if df.empty:
        return df
    for column, key in [
        ("TIPO_EFD", "type"),
        ("ARQUIVO_ID", "files"),
        ("CNPJ_ESTABELECIMENTO", "companies"),
        ("OPERACAO", "operations"),
    ]:
        values = filters.get(key)
        if values:
            df = df[df[column].isin([values] if isinstance(values, str) else values)]
    if not filters.get("include_cancelled", False):
        df = df[~df["CANCELADO"]]
    if filters.get("start"):
        df = df[
            df["DATA"].map(lambda d: d is not None and not pd.isna(d) and d >= filters["start"])
        ]
    if filters.get("end"):
        df = df[df["DATA"].map(lambda d: d is not None and not pd.isna(d) and d <= filters["end"])]
    if filters.get("cfops"):
        df = df[df["CFOPS"].map(lambda s: bool(set(s.split(", ")) & set(filters["cfops"])))]
    if filters.get("search"):
        columns = [
            c
            for c in ("NUM_DOC", "PARTICIPANTE", "CNPJ_PARTICIPANTE", "COD_PART", "ARQUIVO")
            if c in df
        ]
        mask = (
            df[columns]
            .fillna("")
            .astype(str)
            .apply(
                lambda s: s.str.casefold().str.contains(filters["search"].casefold(), regex=False)
            )
            .any(axis=1)
        )
        compact = re.sub(r"[. /-]", "", filters["search"]).casefold()
        if len(compact) >= 6 and compact.isalnum():
            mask |= (
                df["CNPJ_PARTICIPANTE"]
                .fillna("")
                .str.replace(r"[. /-]", "", regex=True)
                .str.casefold()
                .str.contains(compact, regex=False)
            )
        df = df[mask]
    return df.copy()


def calculate_totals(documents):
    if documents.empty:
        return {
            "documents": 0,
            "value": Decimal(0),
            "icms": Decimal(0),
            "pis": Decimal(0),
            "cofins": Decimal(0),
        }
    unique = documents.drop_duplicates("DOCUMENTO_ID")
    unique = unique[~unique["CANCELADO"]]

    def total(col):
        return sum((v for v in unique.get(col, []) if isinstance(v, Decimal)), Decimal(0))

    return {
        "documents": len(unique),
        "value": total("VALOR"),
        "icms": total("VL_ICMS"),
        "pis": total("VL_PIS"),
        "cofins": total("VL_COFINS"),
    }


def main():
    import argparse
    from export import export_workbook

    parser = argparse.ArgumentParser(description="Extrator SPED v6")
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    files = []
    for path in args.files:
        if path.stat().st_size > get_config("processing.max_file_size_mb", 100) * 1024 * 1024:
            parser.error(f"Arquivo acima do limite: {path.name}")
        files.append((path.name, path.read_bytes()))
    batch = process_batch(files, strict=args.strict)
    args.out.write_bytes(export_workbook(batch))
    print(
        json.dumps(
            {"arquivos": len(batch.files), "ocorrencias": len(batch.issues)}, ensure_ascii=True
        )
    )
    if any(i.get("NIVEL") == "Erro" for i in batch.failures):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
