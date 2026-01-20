"""
Extrator SPED - Aplicação Web Streamlit v5.0

Interface web para processamento de arquivos SPED e exportação para Excel.
Suporta EFD ICMS/IPI e EFD Contribuições.

Versão 5.0 - Novas funcionalidades:
- Dashboard de Métricas com gráficos interativos
- Filtros Avançados (período, CFOP, operação)
- Preview de Dados com tabs por bloco
- Upload em Lote de múltiplos arquivos
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from io import BytesIO
import tempfile
import os
from typing import Dict, List, Tuple, Optional
from datetime import datetime

# Importa módulos do projeto
from exceptions import SpedError
from sped_parser import SpedParser, SpedDataProcessor

# Importa layouts específicos
from layouts_icms_ipi import LAYOUTS_ICMS_IPI, NUMERIC_COLUMNS_ICMS_IPI, GROUPS_ICMS_IPI
from layouts_contribuicoes import LAYOUTS_CONTRIBUICOES, NUMERIC_COLUMNS_CONTRIBUICOES, GROUPS_CONTRIBUICOES

# =========================
# CONFIGURAÇÃO DA PÁGINA
# =========================

st.set_page_config(
    page_title="Extrator SPED v5.0",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #1E88E5, #5E35B1);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .success-box {
        padding: 1rem;
        background-color: #E8F5E9;
        border-radius: 0.5rem;
        border-left: 4px solid #4CAF50;
    }
    .info-box {
        padding: 1rem;
        background-color: #E3F2FD;
        border-radius: 0.5rem;
        border-left: 4px solid #2196F3;
    }
    .warning-box {
        padding: 1rem;
        background-color: #FFF3E0;
        border-radius: 0.5rem;
        border-left: 4px solid #FF9800;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #4CAF50, #8BC34A);
    }
    .block-container {
        padding-top: 2rem;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
    }
</style>
""", unsafe_allow_html=True)


# =========================
# FUNÇÕES AUXILIARES
# =========================

def detect_efd_type(file_content: bytes) -> str:
    """Detecta automaticamente o tipo de EFD pelo registro 0000."""
    try:
        content = file_content.decode('latin-1', errors='ignore')
        for line in content.split('\n')[:10]:
            if '|0000|' in line:
                if '|A001|' in content or '|M100|' in content:
                    return 'CONTRIBUICOES'
                return 'ICMS_IPI'
        return 'ICMS_IPI'
    except:
        return 'ICMS_IPI'


def get_layout_config(efd_type: str):
    """Retorna configuração de layout baseado no tipo de EFD."""
    if efd_type == 'CONTRIBUICOES':
        return LAYOUTS_CONTRIBUICOES, NUMERIC_COLUMNS_CONTRIBUICOES, GROUPS_CONTRIBUICOES
    return LAYOUTS_ICMS_IPI, NUMERIC_COLUMNS_ICMS_IPI, GROUPS_ICMS_IPI


def format_currency(value: float) -> str:
    """Formata valor como moeda brasileira."""
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# =========================
# PROCESSAMENTO
# =========================

def process_sped_file(uploaded_file, efd_type: str) -> Tuple[dict, dict, object]:
    """
    Processa um arquivo SPED e retorna os DataFrames consolidados e brutos.
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as tmp:
        tmp.write(uploaded_file.getvalue())
        tmp_path = Path(tmp.name)
    
    try:
        layouts, numeric_cols, groups = get_layout_config(efd_type)
        
        parser = SpedParser(tmp_path, layouts=layouts, numeric_columns=numeric_cols, groups=groups)
        dataframes = parser.parse()
        
        dataframes = SpedDataProcessor.convert_dataframes(dataframes)
        
        consolidated = {}
        
        for group_name, group_config in groups.items():
            parent_code, child_codes, parent_idx, header_idx, header_code = group_config
            
            if parent_code not in dataframes or dataframes[parent_code].empty:
                continue
            
            consolidated_df = SpedDataProcessor.consolidate_group_new(
                dataframes, parent_code, child_codes, parent_idx,
                numeric_columns=numeric_cols
            )
            
            if not consolidated_df.empty:
                if header_code in dataframes and not dataframes[header_code].empty:
                    header_df = dataframes.get(header_code)
                    try:
                        consolidated_df = SpedDataProcessor.attach_header(
                            consolidated_df,
                            header_df,
                            header_idx,
                            f'{header_code}_'
                        )
                    except Exception:
                        pass
                
                consolidated_df.drop(
                    columns=[parent_idx, header_idx],
                    errors='ignore',
                    inplace=True
                )
            
            consolidated[f'{group_name}_CONSOLIDADO'] = consolidated_df
        
        return consolidated, dataframes, parser.metrics
        
    finally:
        if tmp_path.exists():
            os.unlink(tmp_path)


def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """
    Aplica filtros ao DataFrame.
    """
    filtered_df = df.copy()
    
    # Filtro por período
    if filters.get('dt_inicio') and 'DT_DOC' in filtered_df.columns:
        try:
            filtered_df['DT_DOC_PARSED'] = pd.to_datetime(
                filtered_df['DT_DOC'], format='%d%m%Y', errors='coerce'
            )
            filtered_df = filtered_df[
                filtered_df['DT_DOC_PARSED'] >= pd.to_datetime(filters['dt_inicio'])
            ]
            filtered_df = filtered_df.drop(columns=['DT_DOC_PARSED'])
        except:
            pass
    
    if filters.get('dt_fim') and 'DT_DOC' in filtered_df.columns:
        try:
            filtered_df['DT_DOC_PARSED'] = pd.to_datetime(
                filtered_df['DT_DOC'], format='%d%m%Y', errors='coerce'
            )
            filtered_df = filtered_df[
                filtered_df['DT_DOC_PARSED'] <= pd.to_datetime(filters['dt_fim'])
            ]
            filtered_df = filtered_df.drop(columns=['DT_DOC_PARSED'])
        except:
            pass
    
    # Filtro por CFOP
    if filters.get('cfops') and 'CFOP' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['CFOP'].isin(filters['cfops'])]
    
    # Filtro por tipo de operação
    if filters.get('ind_oper') is not None and 'IND_OPER' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['IND_OPER'] == filters['ind_oper']]
    
    # Filtro por CNPJ participante
    if filters.get('cnpj_part') and 'COD_PART' in filtered_df.columns:
        filtered_df = filtered_df[
            filtered_df['COD_PART'].str.contains(filters['cnpj_part'], na=False)
        ]
    
    return filtered_df


def process_multiple_files(uploaded_files: list, efd_type: str, progress_bar) -> Tuple[dict, dict, list]:
    """
    Processa múltiplos arquivos SPED.
    """
    all_consolidated = {}
    all_raw = {}
    all_metrics = []
    
    for idx, uploaded_file in enumerate(uploaded_files):
        progress_bar.progress(
            (idx / len(uploaded_files)),
            text=f"Processando {uploaded_file.name}..."
        )
        
        consolidated, raw, metrics = process_sped_file(uploaded_file, efd_type)
        
        for key, df in consolidated.items():
            if key not in all_consolidated:
                all_consolidated[key] = df
            else:
                all_consolidated[key] = pd.concat([all_consolidated[key], df], ignore_index=True)
        
        for key, df in raw.items():
            if key not in all_raw:
                all_raw[key] = df
            else:
                all_raw[key] = pd.concat([all_raw[key], df], ignore_index=True)
        
        all_metrics.append({
            'arquivo': uploaded_file.name,
            'linhas': metrics.processed_lines,
            'sucesso': metrics.taxa_sucesso,
            'tempo': metrics.tempo_processamento
        })
    
    progress_bar.progress(1.0, text="Concluído!")
    
    return all_consolidated, all_raw, all_metrics


def create_excel_download(dataframes: dict) -> bytes:
    """Cria arquivo Excel em memória para download."""
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in dataframes.items():
            if df is not None and not df.empty:
                safe_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_name, index=False)
    
    output.seek(0)
    return output.getvalue()


# =========================
# DASHBOARD DE MÉTRICAS
# =========================

def render_dashboard(consolidated: dict, raw_dataframes: dict):
    """Renderiza o dashboard de métricas."""
    st.header("📊 Dashboard de Métricas")
    
    # Calcula totais
    totals = calculate_totals(consolidated)
    
    # KPIs principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📄 Total de Documentos",
            value=f"{totals['total_docs']:,}",
            delta=None
        )
    
    with col2:
        st.metric(
            label="💰 Valor Total",
            value=format_currency(totals['vl_total']),
            delta=None
        )
    
    with col3:
        st.metric(
            label="🏦 Total ICMS",
            value=format_currency(totals['vl_icms']),
            delta=None
        )
    
    with col4:
        st.metric(
            label="📈 PIS + COFINS",
            value=format_currency(totals['vl_pis'] + totals['vl_cofins']),
            delta=None
        )
    
    st.divider()
    
    # Gráficos
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        fig_cfop = create_cfop_chart(consolidated)
        if fig_cfop:
            st.plotly_chart(fig_cfop, use_container_width=True)
    
    with col_chart2:
        fig_values = create_values_chart(totals)
        if fig_values:
            st.plotly_chart(fig_values, use_container_width=True)
    
    # Top participantes
    fig_participants = create_top_participants_chart(consolidated)
    if fig_participants:
        st.plotly_chart(fig_participants, use_container_width=True)


def calculate_totals(consolidated: dict) -> dict:
    """Calcula totais dos dados consolidados."""
    totals = {
        'total_docs': 0,
        'vl_total': 0.0,
        'vl_icms': 0.0,
        'vl_pis': 0.0,
        'vl_cofins': 0.0
    }
    
    for key, df in consolidated.items():
        if df is None or df.empty:
            continue
        
        totals['total_docs'] += len(df)
        
        if 'VL_DOC' in df.columns:
            totals['vl_total'] += df['VL_DOC'].sum()
        if 'VL_ICMS' in df.columns:
            totals['vl_icms'] += df['VL_ICMS'].sum()
        if 'VL_PIS' in df.columns:
            totals['vl_pis'] += df['VL_PIS'].sum()
        if 'VL_COFINS' in df.columns:
            totals['vl_cofins'] += df['VL_COFINS'].sum()
    
    return totals


def create_cfop_chart(consolidated: dict):
    """Cria gráfico de barras por CFOP."""
    cfop_data = []
    
    for key, df in consolidated.items():
        if df is None or df.empty:
            continue
        if 'CFOP' in df.columns and 'VL_DOC' in df.columns:
            grouped = df.groupby('CFOP')['VL_DOC'].sum().reset_index()
            grouped['Bloco'] = key.replace('_CONSOLIDADO', '')
            cfop_data.append(grouped)
    
    if not cfop_data:
        return None
    
    all_cfop = pd.concat(cfop_data, ignore_index=True)
    top_cfops = all_cfop.groupby('CFOP')['VL_DOC'].sum().nlargest(10).reset_index()
    
    fig = px.bar(
        top_cfops,
        x='CFOP',
        y='VL_DOC',
        title='🏷️ Top 10 CFOPs por Valor',
        labels={'VL_DOC': 'Valor Total (R$)', 'CFOP': 'CFOP'},
        color='VL_DOC',
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(
        showlegend=False,
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig


def create_values_chart(totals: dict):
    """Cria gráfico de pizza com distribuição de valores."""
    values = [totals['vl_icms'], totals['vl_pis'], totals['vl_cofins']]
    labels = ['ICMS', 'PIS', 'COFINS']
    
    # Remove zeros
    filtered = [(l, v) for l, v in zip(labels, values) if v > 0]
    if not filtered:
        return None
    
    labels, values = zip(*filtered)
    
    fig = px.pie(
        values=values,
        names=labels,
        title='🎯 Distribuição de Tributos',
        color_discrete_sequence=px.colors.qualitative.Set2,
        hole=0.4
    )
    
    fig.update_layout(
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig


def create_top_participants_chart(consolidated: dict):
    """Cria gráfico de top participantes por valor."""
    part_data = []
    
    for key, df in consolidated.items():
        if df is None or df.empty:
            continue
        if 'COD_PART' in df.columns and 'VL_DOC' in df.columns:
            grouped = df.groupby('COD_PART')['VL_DOC'].sum().reset_index()
            part_data.append(grouped)
    
    if not part_data:
        return None
    
    all_parts = pd.concat(part_data, ignore_index=True)
    top_parts = all_parts.groupby('COD_PART')['VL_DOC'].sum().nlargest(10).reset_index()
    
    if top_parts.empty:
        return None
    
    fig = px.bar(
        top_parts,
        x='VL_DOC',
        y='COD_PART',
        orientation='h',
        title='👥 Top 10 Participantes por Valor',
        labels={'VL_DOC': 'Valor Total (R$)', 'COD_PART': 'Código Participante'},
        color='VL_DOC',
        color_continuous_scale='Greens'
    )
    
    fig.update_layout(
        showlegend=False,
        height=400,
        yaxis={'categoryorder': 'total ascending'},
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig


# =========================
# PREVIEW DE DADOS
# =========================

def render_data_preview(consolidated: dict, raw_dataframes: dict, filters: dict):
    """Renderiza preview dos dados em tabs."""
    st.header("📋 Preview dos Dados")
    
    # Cria tabs para cada bloco consolidado
    tabs_names = []
    tabs_data = []
    
    for key, df in consolidated.items():
        if df is not None and not df.empty:
            filtered_df = apply_filters(df, filters)
            tabs_names.append(f"{key.replace('_CONSOLIDADO', '')} ({len(filtered_df):,})")
            tabs_data.append((key, filtered_df))
    
    if not tabs_names:
        st.warning("Nenhum dado consolidado para exibir.")
        return
    
    tabs = st.tabs(tabs_names)
    
    for tab, (key, df) in zip(tabs, tabs_data):
        with tab:
            # Busca
            search_term = st.text_input(
                "🔍 Buscar nos dados:",
                key=f"search_{key}",
                placeholder="Digite para buscar em todas as colunas..."
            )
            
            if search_term:
                mask = df.astype(str).apply(
                    lambda x: x.str.contains(search_term, case=False, na=False)
                ).any(axis=1)
                display_df = df[mask]
            else:
                display_df = df
            
            # Paginação
            page_size = 50
            total_pages = max(1, len(display_df) // page_size + (1 if len(display_df) % page_size else 0))
            
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                page = st.number_input(
                    f"Página (1-{total_pages})",
                    min_value=1,
                    max_value=total_pages,
                    value=1,
                    key=f"page_{key}"
                )
            
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            st.dataframe(
                display_df.iloc[start_idx:end_idx],
                use_container_width=True,
                height=400
            )
            
            st.caption(f"Mostrando {min(end_idx, len(display_df)):,} de {len(display_df):,} registros")


# =========================
# SIDEBAR COM FILTROS
# =========================

def render_sidebar() -> dict:
    """Renderiza sidebar com configurações e filtros."""
    with st.sidebar:
        st.header("⚙️ Configurações")
        
        # Seletor de tipo EFD
        efd_type = st.selectbox(
            "Tipo de EFD",
            options=["Detectar Automaticamente", "EFD ICMS/IPI (Fiscal)", "EFD Contribuições (PIS/COFINS)"],
            index=0,
            help="Selecione o tipo de arquivo EFD ou deixe detectar automaticamente"
        )
        
        st.divider()
        
        # Filtros Avançados
        st.header("🔍 Filtros")
        
        with st.expander("📅 Período", expanded=False):
            dt_inicio = st.date_input(
                "Data Inicial",
                value=None,
                key="filter_dt_inicio"
            )
            dt_fim = st.date_input(
                "Data Final",
                value=None,
                key="filter_dt_fim"
            )
        
        with st.expander("🏷️ CFOP", expanded=False):
            cfop_input = st.text_input(
                "CFOPs (separados por vírgula)",
                placeholder="5102, 6102, 1102",
                key="filter_cfop"
            )
            cfops = [c.strip() for c in cfop_input.split(",") if c.strip()] if cfop_input else []
        
        with st.expander("📊 Operação", expanded=False):
            ind_oper = st.radio(
                "Tipo de Operação",
                options=["Todas", "Entrada (0)", "Saída (1)"],
                index=0,
                key="filter_oper"
            )
            ind_oper_value = None
            if ind_oper == "Entrada (0)":
                ind_oper_value = "0"
            elif ind_oper == "Saída (1)":
                ind_oper_value = "1"
        
        with st.expander("👤 Participante", expanded=False):
            cnpj_part = st.text_input(
                "CNPJ/Código do Participante",
                placeholder="Digite parte do CNPJ ou código",
                key="filter_cnpj"
            )
        
        st.divider()
        
        st.header("ℹ️ Blocos Suportados")
        
        if efd_type == "EFD Contribuições (PIS/COFINS)":
            st.markdown("""
            - **Bloco 0**: Abertura
            - **Bloco A**: Serviços (ISS)
            - **Bloco C**: Docs Fiscais (NFe)
            - **Bloco D**: Transportes
            - **Bloco F**: Demais Docs
            - **Bloco M**: Apuração PIS/COFINS
            """)
        else:
            st.markdown("""
            - **Bloco 0**: Abertura
            - **Bloco C**: NFe/NFCe
            - **Bloco D**: CTe
            - **Bloco E**: Apuração ICMS/IPI
            - **Bloco G**: CIAP
            - **Bloco H**: Inventário
            - **Bloco K**: Produção/Estoque
            - **Bloco 1**: Outras Info
            """)
        
        filters = {
            'dt_inicio': dt_inicio,
            'dt_fim': dt_fim,
            'cfops': cfops,
            'ind_oper': ind_oper_value,
            'cnpj_part': cnpj_part if cnpj_part else None
        }
        
        return efd_type, filters


# =========================
# INTERFACE PRINCIPAL
# =========================

def main():
    # Cabeçalho
    st.markdown('<p class="main-header">📊 Extrator SPED v5.0</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Processe arquivos SPED com Dashboard Interativo, Filtros e Preview</p>', unsafe_allow_html=True)
    
    # Sidebar
    efd_type, filters = render_sidebar()
    
    # Área principal
    col1, col2, col3 = st.columns([1, 3, 1])
    
    with col2:
        # Upload de múltiplos arquivos
        uploaded_files = st.file_uploader(
            "📁 Selecione os arquivos SPED",
            type=['txt', 'sped'],
            help="Você pode selecionar múltiplos arquivos para processamento em lote",
            accept_multiple_files=True
        )
        
        if uploaded_files:
            total_size = sum(len(f.getvalue()) for f in uploaded_files) / 1024
            
            # Detecta tipo automaticamente se necessário
            if efd_type == "Detectar Automaticamente":
                detected_type = detect_efd_type(uploaded_files[0].getvalue())
                type_label = "EFD ICMS/IPI" if detected_type == "ICMS_IPI" else "EFD Contribuições"
                actual_type = detected_type
            elif efd_type == "EFD ICMS/IPI (Fiscal)":
                type_label = "EFD ICMS/IPI"
                actual_type = "ICMS_IPI"
            else:
                type_label = "EFD Contribuições"
                actual_type = "CONTRIBUICOES"
            
            # Info box
            file_names = ", ".join([f.name for f in uploaded_files[:3]])
            if len(uploaded_files) > 3:
                file_names += f" e mais {len(uploaded_files) - 3} arquivo(s)"
            
            st.markdown(f"""
            <div class="info-box">
                <strong>📁 Arquivos:</strong> {len(uploaded_files)} selecionado(s)<br>
                <strong>📄 Nomes:</strong> {file_names}<br>
                <strong>📦 Tamanho Total:</strong> {total_size:.1f} KB<br>
                <strong>🏷️ Tipo:</strong> {type_label}
            </div>
            """, unsafe_allow_html=True)
            
            st.divider()
            
            if st.button("🚀 Processar Arquivo(s)", type="primary", use_container_width=True):
                with st.spinner("Processando arquivos SPED..."):
                    try:
                        progress_bar = st.progress(0, text="Iniciando...")
                        
                        consolidated, raw_dataframes, metrics_list = process_multiple_files(
                            uploaded_files, actual_type, progress_bar
                        )
                        
                        # Aplica filtros
                        filtered_consolidated = {
                            k: apply_filters(v, filters) for k, v in consolidated.items()
                        }
                        
                        # Armazena no session_state
                        st.session_state['consolidated'] = filtered_consolidated
                        st.session_state['raw_dataframes'] = raw_dataframes
                        st.session_state['metrics_list'] = metrics_list
                        st.session_state['filters'] = filters
                        
                        # Resumo do processamento
                        total_lines = sum(m['linhas'] for m in metrics_list)
                        avg_success = sum(m['sucesso'] for m in metrics_list) / len(metrics_list)
                        total_time = sum(m['tempo'] for m in metrics_list)
                        
                        st.markdown(f"""
                        <div class="success-box">
                            <h3>✅ Processamento Concluído!</h3>
                            <p>
                                <strong>Arquivos processados:</strong> {len(metrics_list)}<br>
                                <strong>Linhas processadas:</strong> {total_lines:,}<br>
                                <strong>Taxa de sucesso média:</strong> {avg_success:.2f}%<br>
                                <strong>Tempo total:</strong> {total_time:.2f}s
                            </p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                    except SpedError as e:
                        st.error(f"❌ Erro ao processar arquivo: {e}")
                    except Exception as e:
                        st.error(f"❌ Erro inesperado: {e}")
                        st.exception(e)
    
    # Exibe dashboard e preview se há dados processados
    if 'consolidated' in st.session_state and st.session_state['consolidated']:
        st.divider()
        
        # Dashboard
        render_dashboard(
            st.session_state['consolidated'],
            st.session_state.get('raw_dataframes', {})
        )
        
        st.divider()
        
        # Preview
        render_data_preview(
            st.session_state['consolidated'],
            st.session_state.get('raw_dataframes', {}),
            st.session_state.get('filters', {})
        )
        
        st.divider()
        
        # Estatísticas e Download
        st.header("📥 Exportar Dados")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Blocos Consolidados")
            stats_data = []
            for name, df in st.session_state['consolidated'].items():
                if df is not None and not df.empty:
                    stats_data.append({
                        "Bloco": name,
                        "Registros": len(df),
                        "Colunas": len(df.columns)
                    })
            
            if stats_data:
                stats_df = pd.DataFrame(stats_data)
                st.dataframe(stats_df, use_container_width=True, hide_index=True)
            else:
                st.warning("Nenhum bloco consolidado encontrado.")
        
        with col2:
            st.subheader("⬇️ Download")
            
            excel_bytes = create_excel_download(st.session_state['consolidated'])
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_name = f"sped_consolidado_{timestamp}.xlsx"
            
            st.download_button(
                label="📥 Baixar Excel Consolidado",
                data=excel_bytes,
                file_name=output_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True
            )
    
    # Rodapé
    st.divider()
    st.markdown(
        "<p style='text-align: center; color: #888;'>Extrator SPED v5.0 | Dashboard + Filtros + Preview + Upload em Lote</p>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
