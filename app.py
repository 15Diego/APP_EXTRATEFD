"""
Extrator SPED - Aplicação Web Streamlit

Interface web para processamento de arquivos SPED e exportação para Excel.
Suporta EFD ICMS/IPI e EFD Contribuições.
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from io import BytesIO
import tempfile
import os

# Importa módulos do projeto
from exceptions import SpedError
from Extrat_V3 import SpedParser, SpedDataProcessor

# Importa layouts específicos
from layouts_icms_ipi import LAYOUTS_ICMS_IPI, NUMERIC_COLUMNS_ICMS_IPI, GROUPS_ICMS_IPI
from layouts_contribuicoes import LAYOUTS_CONTRIBUICOES, NUMERIC_COLUMNS_CONTRIBUICOES, GROUPS_CONTRIBUICOES

# =========================
# CONFIGURAÇÃO DA PÁGINA
# =========================

st.set_page_config(
    page_title="Extrator SPED",
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
        color: #1E88E5;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
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
    .stProgress > div > div > div > div {
        background-color: #4CAF50;
    }
</style>
""", unsafe_allow_html=True)


# =========================
# FUNCOES AUXILIARES
# =========================

def detect_efd_type(file_content: bytes) -> str:
    """Detecta automaticamente o tipo de EFD pelo registro 0000."""
    try:
        # Lê primeiras linhas para encontrar registro 0000
        content = file_content.decode('latin-1', errors='ignore')
        for line in content.split('\n')[:10]:
            if '|0000|' in line:
                parts = line.split('|')
                if len(parts) > 2:
                    cod_ver = parts[2] if parts[1] == '0000' else parts[1]
                    # EFD ICMS/IPI geralmente tem COD_VER como número (ex: 018)
                    # EFD Contribuições tem COD_VER diferente
                    # Verificamos também pela estrutura do arquivo
                    if '|A001|' in content or '|M100|' in content:
                        return 'CONTRIBUICOES'
                    return 'ICMS_IPI'
        return 'ICMS_IPI'  # Default
    except:
        return 'ICMS_IPI'


def get_layout_config(efd_type: str):
    """Retorna configuração de layout baseado no tipo de EFD."""
    if efd_type == 'CONTRIBUICOES':
        return LAYOUTS_CONTRIBUICOES, NUMERIC_COLUMNS_CONTRIBUICOES, GROUPS_CONTRIBUICOES
    return LAYOUTS_ICMS_IPI, NUMERIC_COLUMNS_ICMS_IPI, GROUPS_ICMS_IPI


# =========================
# FUNÇÕES DE PROCESSAMENTO
# =========================

def process_sped_file(uploaded_file, efd_type: str) -> dict:
    """
    Processa um arquivo SPED e retorna os DataFrames consolidados.
    """
    # Salva arquivo temporariamente
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as tmp:
        tmp.write(uploaded_file.getvalue())
        tmp_path = Path(tmp.name)
    
    try:
        # Obtém configuração do layout
        layouts, numeric_cols, groups = get_layout_config(efd_type)
        
        # Parse do arquivo com layouts específicos
        parser = SpedParser(tmp_path, layouts=layouts, numeric_columns=numeric_cols, groups=groups)
        dataframes = parser.parse()
        
        # Converte campos numéricos
        dataframes = SpedDataProcessor.convert_dataframes(dataframes)
        
        # Consolida grupos baseado no tipo de EFD
        consolidated = {}
        
        for group_name, group_config in groups.items():
            parent_code, child_codes, parent_idx, header_idx, header_code = group_config
            
            # Verifica se o registro pai existe
            if parent_code not in dataframes or dataframes[parent_code].empty:
                continue
                
            consolidated_df = SpedDataProcessor.consolidate_group(
                dataframes, parent_code, child_codes, parent_idx,
                numeric_columns=numeric_cols
            )
            
            if not consolidated_df.empty:
                # Anexa cabeçalho se existir
                # Anexa cabeçalho se existir
                if header_code in dataframes and not dataframes[header_code].empty:
                    header_df = dataframes.get(header_code)
                    
                    # --- DEBUG VISUAL: Remover em produção ---
                    # st.warning(f"DEBUG: Tentando anexar cabeçalho {header_code} em {parent_code}")
                    # st.write(f"Colunas Consolidado ({len(consolidated_df.columns)}):", consolidated_df.columns.tolist())
                    # st.write(f"Colunas Header ({len(header_df.columns)}):", header_df.columns.tolist())
                    # ----------------------------------------
                    
                    try:
                        consolidated_df = SpedDataProcessor.attach_header(
                            consolidated_df,
                            header_df,
                            header_idx,
                            f'{header_code}_'
                        )
                    except KeyError as e:
                        st.error(f"Erro ao anexar cabeçalho {header_code}: Coluna {e} não encontrada.")
                        st.text(f"Colunas disponíveis no consolidado: {consolidated_df.columns.tolist()}")
                        st.text(f"Colunas disponíveis no {header_code}: {header_df.columns.tolist()}")
                        # Não quebra, segue sem cabeçalho
                        pass
                    except Exception as e:
                        st.error(f"Erro inesperado no cabeçalho: {e}")
                        pass
                
                # Remove colunas de índice
                consolidated_df.drop(
                    columns=[parent_idx, header_idx],
                    errors='ignore',
                    inplace=True
                )
            
            consolidated[f'{group_name}_CONSOLIDADO'] = consolidated_df
        
        return consolidated, parser.metrics
        
    finally:
        if tmp_path.exists():
            os.unlink(tmp_path)


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
# INTERFACE PRINCIPAL
# =========================

def main():
    # Cabeçalho
    st.markdown('<p class="main-header">📊 Extrator SPED</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Processe arquivos SPED e exporte dados consolidados para Excel</p>', unsafe_allow_html=True)
    
    # Sidebar
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
        
        st.divider()
        
        st.header("📋 Instruções")
        st.markdown("""
        1. Selecione o tipo de EFD
        2. Faça upload do arquivo
        3. Clique em Processar
        4. Baixe o Excel
        """)
    
    # Área principal
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        # Upload de arquivo
        uploaded_file = st.file_uploader(
            "📁 Selecione o arquivo SPED",
            type=['txt', 'sped'],
            help="Arquivo SPED no formato .txt ou .sped"
        )
        
        if uploaded_file is not None:
            file_size = len(uploaded_file.getvalue()) / 1024
            
            # Detecta tipo automaticamente se necessário
            if efd_type == "Detectar Automaticamente":
                detected_type = detect_efd_type(uploaded_file.getvalue())
                type_label = "EFD ICMS/IPI" if detected_type == "ICMS_IPI" else "EFD Contribuições"
                actual_type = detected_type
            elif efd_type == "EFD ICMS/IPI (Fiscal)":
                type_label = "EFD ICMS/IPI"
                actual_type = "ICMS_IPI"
            else:
                type_label = "EFD Contribuições"
                actual_type = "CONTRIBUICOES"
            
            st.markdown(f"""
            <div class="info-box">
                <strong>Arquivo:</strong> {uploaded_file.name}<br>
                <strong>Tamanho:</strong> {file_size:.1f} KB<br>
                <strong>Tipo:</strong> {type_label}
            </div>
            """, unsafe_allow_html=True)
            
            st.divider()
            
            if st.button("🚀 Processar Arquivo", type="primary", use_container_width=True):
                with st.spinner("Processando arquivo SPED..."):
                    try:
                        progress_bar = st.progress(0, text="Iniciando...")
                        
                        progress_bar.progress(20, text="Parseando arquivo...")
                        consolidated, metrics = process_sped_file(uploaded_file, actual_type)
                        
                        progress_bar.progress(60, text="Gerando Excel...")
                        excel_bytes = create_excel_download(consolidated)
                        
                        progress_bar.progress(100, text="Concluído!")
                        
                        st.markdown(f"""
                        <div class="success-box">
                            <h3>✅ Processamento Concluído!</h3>
                            <p>
                                <strong>Linhas processadas:</strong> {metrics.processed_lines:,}<br>
                                <strong>Taxa de sucesso:</strong> {metrics.taxa_sucesso:.2f}%<br>
                                <strong>Tempo:</strong> {metrics.tempo_processamento:.2f}s
                            </p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Estatísticas dos blocos
                        st.subheader("📊 Blocos Consolidados")
                        
                        stats_data = []
                        for name, df in consolidated.items():
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
                        
                        st.divider()
                        
                        output_name = uploaded_file.name.replace('.txt', '').replace('.sped', '') + '_consolidado.xlsx'
                        
                        st.download_button(
                            label="📥 Baixar Excel",
                            data=excel_bytes,
                            file_name=output_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            type="primary",
                            use_container_width=True
                        )
                        
                    except SpedError as e:
                        st.error(f"❌ Erro ao processar arquivo: {e}")
                    except Exception as e:
                        st.error(f"❌ Erro inesperado: {e}")
                        st.exception(e)
    
    # Rodapé
    st.divider()
    st.markdown(
        "<p style='text-align: center; color: #888;'>Extrator SPED v4.4 (Debug Mode) | Suporte Multi-EFD</p>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
