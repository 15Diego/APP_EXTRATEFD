"""
Extrator SPED - Aplicação Web Streamlit

Interface web para processamento de arquivos SPED e exportação para Excel.
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from io import BytesIO
import tempfile
import os

# Importa módulos do projeto
from exceptions import SpedError
from Extrat_V3 import SpedParser, SpedDataProcessor, GROUPS

# =========================
# CONFIGURAÇÃO DA PÁGINA
# =========================

st.set_page_config(
    page_title="Extrator SPED",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado para melhorar a aparência
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
    .stProgress > div > div > div > div {
        background-color: #4CAF50;
    }
</style>
""", unsafe_allow_html=True)


# =========================
# FUNÇÕES DE PROCESSAMENTO
# =========================

def process_sped_file(uploaded_file) -> dict:
    """
    Processa um arquivo SPED e retorna os DataFrames consolidados.
    
    Args:
        uploaded_file: Arquivo carregado via Streamlit
        
    Returns:
        Dicionário com DataFrames consolidados
    """
    # Salva arquivo temporariamente
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as tmp:
        tmp.write(uploaded_file.getvalue())
        tmp_path = Path(tmp.name)
    
    try:
        # Parse do arquivo
        parser = SpedParser(tmp_path)
        dataframes = parser.parse()
        
        # Converte campos numéricos
        dataframes = SpedDataProcessor.convert_dataframes(dataframes)
        
        # Consolida grupos
        consolidated = {}
        
        for group_name, (parent_code, child_codes, parent_idx, header_idx, header_code) in GROUPS.items():
            consolidated_df = SpedDataProcessor.consolidate_group(
                dataframes, parent_code, child_codes, parent_idx
            )
            
            if not consolidated_df.empty:
                # Anexa cabeçalho
                consolidated_df = SpedDataProcessor.attach_header(
                    consolidated_df,
                    dataframes.get(header_code),
                    header_idx,
                    f'{header_code}_'
                )
                
                # Remove colunas de índice
                consolidated_df.drop(
                    columns=[parent_idx, header_idx],
                    errors='ignore',
                    inplace=True
                )
            
            consolidated[f'{group_name}_CONSOLIDADO'] = consolidated_df
        
        return consolidated, parser.metrics
        
    finally:
        # Limpa arquivo temporário
        if tmp_path.exists():
            os.unlink(tmp_path)


def create_excel_download(dataframes: dict) -> bytes:
    """
    Cria arquivo Excel em memória para download.
    
    Args:
        dataframes: Dicionário com DataFrames
        
    Returns:
        Bytes do arquivo Excel
    """
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in dataframes.items():
            if df is not None and not df.empty:
                # Limita nome da planilha a 31 caracteres (limite do Excel)
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
    
    # Sidebar com informações
    with st.sidebar:
        st.header("ℹ️ Sobre")
        st.markdown("""
        **Extrator SPED v3.0**
        
        Processa arquivos SPED e consolida os seguintes blocos:
        
        - **Bloco C**: NFe/NFCe
        - **Bloco C500**: Energia Elétrica
        - **Bloco D**: CTe
        - **Bloco D500**: Telecom
        - **Bloco D700**: NFCom
        - **Bloco A**: Serviços
        - **Bloco F**: Demais Documentos
        - **Bloco E**: Apuração ICMS
        """)
        
        st.divider()
        
        st.header("📋 Instruções")
        st.markdown("""
        1. Faça upload do arquivo SPED (.txt)
        2. Aguarde o processamento
        3. Baixe o Excel com os dados consolidados
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
            # Informações do arquivo
            file_size = len(uploaded_file.getvalue()) / 1024
            st.markdown(f"""
            <div class="info-box">
                <strong>Arquivo:</strong> {uploaded_file.name}<br>
                <strong>Tamanho:</strong> {file_size:.1f} KB
            </div>
            """, unsafe_allow_html=True)
            
            st.divider()
            
            # Botão de processamento
            if st.button("🚀 Processar Arquivo", type="primary", use_container_width=True):
                with st.spinner("Processando arquivo SPED..."):
                    try:
                        # Processa o arquivo
                        progress_bar = st.progress(0, text="Iniciando...")
                        
                        progress_bar.progress(20, text="Parseando arquivo...")
                        consolidated, metrics = process_sped_file(uploaded_file)
                        
                        progress_bar.progress(60, text="Gerando Excel...")
                        excel_bytes = create_excel_download(consolidated)
                        
                        progress_bar.progress(100, text="Concluído!")
                        
                        # Sucesso
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
                            st.warning("Nenhum bloco consolidado encontrado no arquivo.")
                        
                        st.divider()
                        
                        # Botão de download
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
        "<p style='text-align: center; color: #888;'>Extrator SPED v3.0 | Desenvolvido com Streamlit</p>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
