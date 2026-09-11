"""Extrator SPED v6 — an explicit, session-local review workflow."""

from decimal import Decimal
import pandas as pd
import plotly.express as px
import streamlit as st
from schema import TYPES
from sped_parser import (
    process_batch,
    filter_documents,
    calculate_totals,
    conflicting_files,
    get_config,
)
from export import export_workbook
from demo import sample_file

st.set_page_config(page_title="Extrator SPED · Workspace fiscal", page_icon="◈", layout="wide")
st.markdown(
    """<style>
.stApp {background:#f5f7fa;color:#20333b;}
.block-container {max-width:1440px;padding-top:4.5rem;padding-bottom:3rem;}
[data-testid="stSidebar"] {background:#142f37;}
[data-testid="stSidebar"] * {color:#f2f7f8;}
[data-testid="stSidebar"] input {color:#142f37;}
[data-testid="stSidebar"] button p {color:#142f37;}
.brand {font-size:1.5rem;font-weight:750;letter-spacing:-.04em;margin-bottom:3px;}
.eyebrow {color:#21766c;font-size:.76rem;font-weight:750;letter-spacing:.12em;text-transform:uppercase;}
.hero {font-size:2.35rem;font-weight:750;letter-spacing:-.055em;line-height:1.15;margin:10px 0;}
.lede {color:#596d77;font-size:1rem;line-height:1.6;max-width:740px;}
.side-note {font-size:.85rem;line-height:1.6;opacity:.8;}
[data-testid="stMetric"] {background:white;border:1px solid #dce5e9;border-radius:12px;padding:18px 20px;}
[data-testid="stMetricValue"] {font-size:1.7rem;}
.stTabs [data-baseweb="tab-list"] {gap:1rem;}
.stTabs [aria-selected="true"] {color:#147969;font-weight:700;}
button[kind="primary"] {background:#147969;border-color:#147969;}
button:focus-visible,input:focus-visible {outline:3px solid #d49b23!important;outline-offset:3px;}
.step {background:white;border:1px solid #dce5e9;border-radius:12px;padding:24px;min-height:160px;}
.step b {display:block;font-size:1.1rem;margin:8px 0;}
.step p {color:#60747e;font-size:.94rem;line-height:1.5;}
@media(max-width:700px){.hero{font-size:1.8rem}.block-container{padding:4.5rem 1rem 1rem}[data-testid="stMetricValue"]{font-size:1.3rem}}
</style>""",
    unsafe_allow_html=True,
)


def money(value):
    return "R$ " + f"{value:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")


def display_table(df):
    """Convert Decimal for presentation only; calculations and exports retain precision."""
    frame = df.copy()
    for col in frame.columns:
        frame[col] = frame[col].map(lambda v: float(v) if isinstance(v, Decimal) else v)
    return frame


def clear_filters():
    for key in list(st.session_state):
        if key.startswith("f_") or key.startswith("page_") or key.startswith("export_"):
            del st.session_state[key]


def reset_workspace():
    clear_filters()
    st.session_state.pop("batch", None)
    st.session_state.pop("demo_mode", None)
    st.session_state.pop("rejected_report", None)
    st.session_state["upload_epoch"] = st.session_state.get("upload_epoch", 0) + 1


def set_batch(batch, demo=False):
    clear_filters()
    st.session_state["batch"] = batch
    st.session_state["demo_mode"] = demo


def paginate(df, key, columns=None):
    if df.empty:
        st.info("Nenhum registro nesta seleção. Ajuste os filtros ou consulte a qualidade do lote.")
        return
    if columns:
        df = df[[c for c in columns if c in df]]
    pages = max(1, (len(df) + 49) // 50)
    state_key = f"page_{key}"
    if st.session_state.get(state_key, 1) > pages:
        st.session_state[state_key] = 1
    page = st.number_input("Página", min_value=1, max_value=pages, step=1, key=state_key)
    start = (page - 1) * 50
    st.dataframe(display_table(df.iloc[start : start + 50]), hide_index=True, width="stretch")
    st.caption(f"{start + 1}–{min(start + 50, len(df))} de {len(df):,} registros · 50 por página")


def render_import():
    with st.expander("Importar arquivos", expanded="batch" not in st.session_state):
        st.write(
            "Selecione um ou mais arquivos. Cada EFD será identificada pelo próprio cabeçalho."
        )
        files = st.file_uploader(
            "Arquivos SPED",
            type=["txt", "sped"],
            accept_multiple_files=True,
            key=f"uploads_{st.session_state.get('upload_epoch', 0)}",
        )
        a, b, c = st.columns([2, 2, 3])
        kind = a.selectbox(
            "Tipo de arquivo",
            ["auto", *TYPES],
            format_func=lambda v: "Detectar por arquivo" if v == "auto" else TYPES[v],
        )
        codec = b.selectbox(
            "Codificação",
            ["auto", "utf-8-sig", "cp1252", "latin-1"],
            format_func=lambda v: "Automática" if v == "auto" else v,
        )
        strict = c.checkbox(
            "Modo estrito",
            help="Interrompe a aceitação de um arquivo se houver qualquer erro ou aviso. Os demais arquivos continuam sendo processados.",
        )
        st.caption(
            f"Até {get_config('processing.max_files', 20)} arquivos · {get_config('processing.max_file_size_mb', 100)} MB por arquivo · {get_config('processing.max_batch_size_mb', 200)} MB por lote. O novo processamento substitui o lote da sessão."
        )
        if st.button("Processar arquivos", type="primary", disabled=not files):
            reset_workspace()
            try:
                progress = st.progress(0, text="Preparando arquivos…")
                with st.spinner("Lendo registros e verificando a qualidade…"):
                    batch = process_batch(
                        [(f.name, f.getvalue()) for f in files],
                        kind,
                        strict,
                        codec,
                        lambda fraction, name: progress.progress(fraction, text=f"Lendo {name}"),
                    )
                set_batch(batch)
                st.rerun()
            except (ValueError, OSError) as exc:
                st.error(str(exc))


def render_filters(batch):
    manifest = batch.manifest
    with st.container(border=True):
        title, action = st.columns([5, 1])
        title.markdown("**Refinar documentos**")
        action.button("Limpar filtros", on_click=clear_filters, width="stretch")
        a, b, c = st.columns(3)
        kinds = sorted(manifest["TIPO_EFD"].unique())
        kind = a.selectbox("Escrituração", kinds, format_func=lambda v: TYPES[v], key="f_type")
        choices = manifest[manifest["TIPO_EFD"] == kind]
        labels = {
            r["ARQUIVO_ID"]: f"{r['ARQUIVO']} · {r['ARQUIVO_ID'][:6]}"
            for r in choices.to_dict("records")
        }
        if "f_files" in st.session_state:
            st.session_state["f_files"] = [v for v in st.session_state["f_files"] if v in labels]
        files = b.multiselect(
            "Arquivos",
            list(labels),
            format_func=labels.get,
            key="f_files",
            placeholder="Todos desta escrituração",
        )
        operations = c.multiselect(
            "Operação",
            ["Entrada", "Saída", "Outra", "Não informada"],
            key="f_operations",
            placeholder="Todas as operações",
        )
        a, b, c, d = st.columns([1, 1, 2, 2])
        start = a.date_input("De", value=None, key="f_start", format="DD/MM/YYYY")
        end = b.date_input("Até", value=None, key="f_end", format="DD/MM/YYYY")
        cfops = c.text_input("CFOPs", placeholder="1102, 5102", key="f_cfops")
        search = d.text_input(
            "Documento ou participante", placeholder="Número, nome, código ou CNPJ", key="f_search"
        )
        companies = sorted(
            batch.documents.get("CNPJ_ESTABELECIMENTO", pd.Series(dtype=str)).dropna().unique()
        )
        a, b = st.columns([3, 2])
        company = a.multiselect(
            "Estabelecimentos",
            companies,
            key="f_companies",
            placeholder="Todos os estabelecimentos",
        )
        include_cancelled = b.checkbox(
            "Mostrar cancelados e denegados",
            key="f_cancelled",
            help="Esses registros ficam disponíveis para consulta, mas não entram nos indicadores.",
        )
        st.caption(
            "CFOP seleciona documentos que contêm o código. Os valores continuam sendo do documento inteiro. Cada escrituração é analisada separadamente para evitar dupla contagem."
        )
    if start and end and start > end:
        st.error("A data inicial deve ser anterior ou igual à data final.")
        return None
    selected = files or list(labels)
    return {
        "type": kind,
        "files": selected,
        "operations": operations,
        "start": start,
        "end": end,
        "cfops": [v.strip() for v in cfops.split(",") if v.strip()],
        "search": search,
        "companies": company,
        "include_cancelled": include_cancelled,
    }


def render_overview(docs, conflicts):
    if conflicts:
        st.error(
            "Há arquivos da mesma empresa e escrituração com períodos sobrepostos. Selecione apenas a versão desejada no filtro Arquivos para liberar os indicadores."
        )
        st.dataframe(pd.DataFrame(conflicts, columns=["Arquivo", "Sobreposto a"]), hide_index=True)
        return
    totals = calculate_totals(docs)
    for column, label, key in zip(
        st.columns(4),
        [
            "Documentos e operações",
            "Valor dos documentos",
            "ICMS informado",
            "PIS + COFINS informado",
        ],
        ["documents", "value", "icms", "contributions"],
    ):
        value = totals["pis"] + totals["cofins"] if key == "contributions" else totals[key]
        column.metric(label, f"{value:,}" if key == "documents" else money(value))
    st.caption(
        "Contagem por registro de documento/operação aceito, sem repetição por itens. Tributos informados no cabeçalho; não equivalem à apuração. Registros agregados e rejeitados não entram nestes indicadores."
    )
    if docs.empty:
        st.info(
            "Nada corresponde aos filtros atuais. Os registros completos continuam disponíveis na aba Registros."
        )
        return
    valid = docs[~docs["CANCELADO"]].copy()
    if valid.empty:
        return
    a, b = st.columns([3, 2])
    with a:
        st.markdown("#### Movimento no período")
        grouped = (
            valid.groupby("DATA", dropna=True)["VALOR"]
            .agg(lambda s: sum((v for v in s if isinstance(v, Decimal)), Decimal(0)))
            .reset_index()
        )
        grouped["VALOR"] = grouped["VALOR"].map(float)
        fig = px.bar(
            grouped,
            x="DATA",
            y="VALOR",
            color_discrete_sequence=["#218575"],
            labels={"DATA": "Data", "VALOR": "Valor (R$)"},
        )
        fig.update_layout(
            height=310,
            margin=dict(l=0, r=0, t=15, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )
        st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    with b:
        st.markdown("#### Participantes por valor")
        grouped = (
            valid.groupby(
                ["CNPJ_ESTABELECIMENTO", "PARTICIPANTE_ID", "PARTICIPANTE"], dropna=False
            )["VALOR"]
            .agg(lambda s: sum((v for v in s if isinstance(v, Decimal)), Decimal(0)))
            .reset_index()
        )
        grouped["VALOR"] = grouped["VALOR"].map(float)
        st.dataframe(
            grouped.sort_values("VALOR", ascending=False).head(8)[["PARTICIPANTE", "VALOR"]],
            column_config={
                "PARTICIPANTE": "Participante",
                "VALOR": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f"),
            },
            hide_index=True,
            width="stretch",
        )


def main():
    with st.sidebar:
        st.markdown(
            '<div class="brand">◈ extrator<span style="color:#73caba">.</span></div>',
            unsafe_allow_html=True,
        )
        st.caption("SPED WORKSPACE / v6.0")
        st.divider()
        st.markdown("**Seu fluxo de conferência**")
        st.write("1 · Importe suas escriturações")
        st.write("2 · Revise dados e ocorrências")
        st.write("3 · Exporte com rastreabilidade")
        st.divider()
        st.markdown(
            '<p class="side-note">Os dados ficam na sessão deste servidor. Não são enviados a serviços de IA nem compartilhados entre sessões pela aplicação.</p>',
            unsafe_allow_html=True,
        )
        st.button("Limpar sessão", on_click=reset_workspace, width="stretch")
        st.caption("Em servidor compartilhado, configure autenticação na hospedagem e HTTPS.")
    st.markdown(
        '<div class="eyebrow">Workspace fiscal</div><div class="hero">Da escrituração à informação.</div><p class="lede">Explore seus arquivos SPED com documentos únicos, detalhes preservados e uma trilha clara até a origem.</p>',
        unsafe_allow_html=True,
    )
    render_import()
    batch = st.session_state.get("batch")
    if batch is None:
        st.write("")
        for col, number, title, body in zip(
            st.columns(3),
            ["01", "02", "03"],
            ["Importação inteligente", "Qualidade visível", "Excel organizado"],
            [
                "Identificação por arquivo e proteção contra duplicatas.",
                "Erros, avisos e registros não suportados sempre à vista.",
                "Documentos, detalhes e originais em abas separadas.",
            ],
        ):
            col.markdown(
                f'<div class="step"><span class="eyebrow">{number}</span><b>{title}</b><p>{body}</p></div>',
                unsafe_allow_html=True,
            )
        st.write("")
        st.info(
            "Primeira vez por aqui? Explore uma demonstração com dados fictícios, sem precisar enviar arquivos."
        )
        if st.button("Explorar demonstração"):
            set_batch(process_batch([("demonstracao.txt", sample_file())]), demo=True)
            st.rerun()
        return
    if st.session_state.get("demo_mode"):
        st.info("Modo demonstração · todos os dados abaixo são fictícios.")
    if not batch.files:
        st.error(
            "Nenhum arquivo pôde ser aceito. Confira os motivos abaixo e envie uma nova seleção."
        )
        st.dataframe(batch.issues, hide_index=True, width="stretch")
        if st.button("Preparar relatório de rejeições"):
            try:
                st.session_state["rejected_report"] = export_workbook(batch)
            except ValueError as exc:
                st.error(str(exc))
        if "rejected_report" in st.session_state:
            st.download_button(
                "Baixar relatório de rejeições",
                st.session_state["rejected_report"],
                "sped_rejeicoes.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        return
    issues = batch.issues
    if not issues.empty:
        st.warning(
            f"{len(issues):,} ocorrência(s) no lote. Consulte Qualidade antes de usar os resultados; registros rejeitados e não suportados ficam fora dos indicadores."
        )
    else:
        st.success(
            f"{len(batch.files)} arquivo(s) processado(s) sem ocorrências nas verificações implementadas."
        )
    filters = render_filters(batch)
    if filters is None:
        return
    docs = filter_documents(batch, filters)
    conflicts = conflicting_files(batch, filters["files"])
    overview, documents, records, quality, download = st.tabs(
        ["Visão geral", "Documentos", "Registros", "Qualidade", "Exportar"]
    )
    with overview:
        render_overview(docs, conflicts)
    with documents:
        st.markdown("### Documentos da seleção")
        paginate(
            docs,
            "documents",
            [
                "DATA",
                "NUM_DOC",
                "REGISTRO",
                "OPERACAO",
                "PARTICIPANTE",
                "CNPJ_PARTICIPANTE",
                "VALOR",
                "CFOPS",
                "CANCELADO",
                "ARQUIVO",
                "LINHA",
            ],
        )
        if not docs.empty:
            with st.expander("Inspecionar um documento e seus detalhes"):
                choices = docs.set_index("DOCUMENTO_ID")
                selected = st.selectbox(
                    "Documento",
                    list(choices.index),
                    format_func=lambda v: f"{choices.loc[v, 'ARQUIVO']} · linha {choices.loc[v, 'LINHA']}",
                    key="document_detail",
                )
                for f in batch.files:
                    for code, table in f.tables.items():
                        if "DOCUMENTO_ID" in table:
                            detail = table[table["DOCUMENTO_ID"] == selected]
                            if not detail.empty:
                                st.markdown(f"**{code} · {len(detail)} registro(s)**")
                                st.dataframe(
                                    display_table(detail), hide_index=True, width="stretch"
                                )
    with records:
        st.markdown("### Registros completos do lote")
        st.caption(
            "Esta área não usa os filtros de documentos. Inclui apuração, cadastros e registros sem documento associado."
        )
        a, b, c = st.columns(3)
        file_index = a.selectbox(
            "Arquivo de origem",
            range(len(batch.files)),
            format_func=lambda i: f"{batch.files[i].metadata['ARQUIVO']} · {batch.files[i].metadata['ARQUIVO_ID'][:6]}",
        )
        original = b.toggle(
            "Ver conteúdo original",
            value=False,
            help="Inclui registros rejeitados, não suportados e campos adicionais.",
        )
        tables = batch.files[file_index].raw_tables if original else batch.files[file_index].tables
        code = c.selectbox("Registro", sorted(tables))
        if code:
            search = st.text_input(
                "Buscar neste registro",
                key="record_search",
                placeholder="Busca literal em qualquer coluna",
            )
            frame = tables[code]
            if search:
                frame = frame[
                    frame.fillna("")
                    .astype(str)
                    .apply(lambda s: s.str.contains(search, case=False, regex=False))
                    .any(axis=1)
                ]
            paginate(frame, f"record_{file_index}_{original}_{code}")
    with quality:
        st.markdown("### Qualidade e origem")
        st.caption(
            "Aceito significa que passou pelas verificações implementadas; não é uma homologação fiscal. O catálogo pode divergir de versões históricas ou recentes: essas linhas são preservadas para revisão."
        )
        st.dataframe(batch.manifest, hide_index=True, width="stretch")
        if issues.empty:
            st.success("Nenhuma ocorrência registrada neste lote.")
        else:
            levels = st.multiselect(
                "Severidade", sorted(issues["NIVEL"].unique()), key="quality_levels"
            )
            paginate(issues[issues["NIVEL"].isin(levels)] if levels else issues, "quality")
        all_conflicts = conflicting_files(batch)
        if all_conflicts:
            st.warning(
                "Períodos sobrepostos detectados. Confira arquivos originais e retificadores antes de escolher quais analisar."
            )
            st.dataframe(
                pd.DataFrame(all_conflicts, columns=["Arquivo", "Sobreposto a"]), hide_index=True
            )
    with download:
        st.markdown("### Uma exportação para cada necessidade")
        scope = st.radio(
            "Escopo", ["Lote completo", "Documentos filtrados"], horizontal=True, key="export_scope"
        )
        filtered = scope == "Documentos filtrados"
        st.caption(
            "Lote completo inclui todas as linhas em abas de registros originais, além de documentos, registros aceitos, arquivos e ocorrências. Documentos filtrados inclui apenas a seleção atual."
        )
        fingerprint = repr((filters, scope, tuple(f.metadata["ARQUIVO_ID"] for f in batch.files)))
        if st.session_state.get("export_fingerprint") != fingerprint:
            st.session_state.pop("export_bytes", None)
        if st.button(
            "Preparar Excel", type="primary", disabled=filtered and (docs.empty or bool(conflicts))
        ):
            try:
                with st.spinner("Organizando as abas do Excel…"):
                    st.session_state["export_bytes"] = export_workbook(
                        batch, docs if filtered else None, filtered
                    )
                    st.session_state["export_fingerprint"] = fingerprint
            except (ValueError, OSError) as exc:
                st.error(str(exc))
        if "export_bytes" in st.session_state:
            st.download_button(
                "Baixar Excel",
                st.session_state["export_bytes"],
                file_name="sped_selecao.xlsx" if filtered else "sped_completo.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
            )
        if filtered and docs.empty:
            st.info(
                "Não há documentos para exportar com estes filtros. O lote completo continua disponível."
            )
    st.divider()
    st.caption(
        "Extrator SPED v6 · Conferência com origem preservada · Verifique as ocorrências e valide a escrituração no PVA aplicável."
    )


if __name__ == "__main__":
    main()
