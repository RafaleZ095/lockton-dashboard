import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from collections import Counter
from io import BytesIO
import re

# ============================================
# CONFIGURAÇÃO DA PÁGINA
# ============================================
st.set_page_config(
    page_title="Lockton Analytics - Movimentação de Beneficiários",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# FUNÇÕES DE ANÁLISE
# ============================================

def extract_keywords_from_subject(df):
    """Extrai palavras-chave da coluna ASSUNTO"""
    keywords = {
        'cancelamento': ['cancel', 'exclusão', 'excluir', 'deslig'],
        'reembolso': ['reembolso', 'estorno', 'devolução'],
        'inclusão': ['inclu', 'adesão', 'cadastr'],
        'extensão': ['extensão', 'continuidade', 'manutenção'],
        'carência': ['carência', 'carencia', 'prazo'],
        'reintegração': ['reintegr', 'reativa', 'ativar'],
        'urgente': ['urgent', 'emergência', 'asap'],
        'portabilidade': ['portabilidade', 'transferência', 'migraç'],
        'judicial': ['judicial', 'ação', 'liminar', 'mandado']
    }
    
    def classify_subject(text):
        if pd.isna(text):
            return 'outros'
        text_lower = str(text).lower()
        for category, patterns in keywords.items():
            if any(pattern in text_lower for pattern in patterns):
                return category
        return 'outros'
    
    df['PALAVRA_CHAVE'] = df['ASSUNTO'].apply(classify_subject)
    return df

def get_performance_by_responsavel(df):
    """Análise de performance por responsável"""
    resp_stats = df.groupby('RESPONSÁVEL').agg({
        'PROTOCOLO': 'count',
        'TEMPO_RESOLUCAO': 'mean',
        'SLA': lambda x: (x == 'Dentro do prazo').mean() * 100
    }).round(1).reset_index()
    resp_stats.columns = ['Responsável', 'Total Chamados', 'Tempo Médio (dias)', '% SLA OK']
    resp_stats = resp_stats[resp_stats['Responsável'].notna() & (resp_stats['Responsável'] != '')]
    return resp_stats.sort_values('Total Chamados', ascending=False).head(10)

def get_reincidencia_analysis(df):
    """Análise de reincidência por CPF"""
    if 'CPF BENEFICIARIO' not in df.columns:
        return pd.DataFrame()
    
    cpf_counts = df['CPF BENEFICIARIO'].value_counts().reset_index()
    cpf_counts.columns = ['CPF', 'Quantidade']
    cpf_counts = cpf_counts[cpf_counts['CPF'].notna() & (cpf_counts['CPF'] != '')]
    
    # Identificar reincidentes (mais de 3 chamados)
    cpf_counts['Tipo'] = cpf_counts['Quantidade'].apply(
        lambda x: 'Alta reincidência' if x > 5 else ('Média reincidência' if x > 2 else 'Baixa reincidência')
    )
    return cpf_counts.head(10)

def get_gargalo_analysis(df):
    """Identifica gargalos por categoria"""
    if 'CATEGORIA' not in df.columns:
        return pd.DataFrame()
    
    gargalos = df.groupby('CATEGORIA').agg({
        'TEMPO_RESOLUCAO': 'mean',
        'PROTOCOLO': 'count',
        'SLA': lambda x: (x == 'Dentro do prazo').mean() * 100
    }).round(1).reset_index()
    gargalos.columns = ['Categoria', 'Tempo Médio (dias)', 'Total Chamados', '% SLA OK']
    gargalos = gargalos[gargalos['Categoria'].notna() & (gargalos['Categoria'] != 'Não informado')]
    return gargalos.sort_values('Tempo Médio (dias)', ascending=False).head(8)

def get_summary_stats(df):
    """Retorna estatísticas resumidas"""
    total = len(df)
    dentro_sla = len(df[df['SLA'] == 'Dentro do prazo'])
    fora_sla = len(df[df['SLA'] == 'Fora do prazo'])
    nao_aplicavel = len(df[df['SLA'] == 'Não aplicável'])
    nao_informado = len(df[df['SLA'] == 'Não informado'])
    
    def calc_percent(count, total):
        if total == 0:
            return "0 (0.0%)"
        return f"{count:,} ({round(count/total*100, 1)}%)"
    
    stats = {
        'Total de Chamados': f"{total:,}",
        'Chamados Abertos': f"{len(df[df['STATUS'] == 'Aberto']):,}",
        'Chamados Concluídos': f"{len(df[df['STATUS'] == 'Concluído']):,}",
        'Chamados Cancelados': f"{len(df[df['STATUS'] == 'Cancelado']):,}",
        'Dentro do SLA': calc_percent(dentro_sla, total),
        'Fora do SLA': calc_percent(fora_sla, total),
        'SLA Não Aplicável': calc_percent(nao_aplicavel, total),
        'SLA Não Informado': calc_percent(nao_informado, total),
        'Tempo Médio Resolução': f"{round(df[df['TEMPO_RESOLUCAO'].notna()]['TEMPO_RESOLUCAO'].mean(), 1)} dias" if len(df[df['TEMPO_RESOLUCAO'].notna()]) > 0 else "N/A",
        'Período Analisado': f"{df['ABERTURA_DT'].min().strftime('%d/%m/%Y')} a {df['ABERTURA_DT'].max().strftime('%d/%m/%Y')}" if pd.notna(df['ABERTURA_DT'].min()) else "N/A"
    }
    return stats

# ============================================
# FUNÇÕES DE PROCESSAMENTO (CACHEADAS)
# ============================================

@st.cache_data
def load_and_process_data(uploaded_file):
    """Carrega e processa o arquivo Excel - executado apenas uma vez"""
    if uploaded_file is not None:
        with st.spinner("Carregando e processando dados..."):
            df = pd.read_excel(uploaded_file, sheet_name=0)
            
            # Processar datas
            df['ABERTURA_DT'] = pd.to_datetime(df['ABERTURA'], errors='coerce')
            df['FECHAMENTO_DT'] = pd.to_datetime(df['FECHAMENTO'], errors='coerce')
            
            # Extrair mês/ano
            df['MES_ABERTURA'] = df['ABERTURA_DT'].dt.to_period('M').astype(str)
            df['ANO_MES'] = df['ABERTURA_DT'].dt.strftime('%Y-%m')
            df['MES_NOME'] = df['ABERTURA_DT'].dt.strftime('%B')
            df['ANO'] = df['ABERTURA_DT'].dt.year
            df['DIA_SEMANA'] = df['ABERTURA_DT'].dt.day_name()
            df['HORA'] = df['ABERTURA_DT'].dt.hour
            
            # ============================================
            # NOVO CÁLCULO DE SLA PARA A EQUIPE TÉCNICA
            # ============================================
            equipes_validas = ['Lockton', 'JBS', 'Lockton e JBS']
            df['DATA_INICIO_SLA'] = df.apply(
                lambda row: row['ABERTURA_DT'] if row['EQUIPE'] in equipes_validas else pd.NaT,
                axis=1
            )
            df['TEMPO_RESOLUCAO_SLA'] = (df['FECHAMENTO_DT'] - df['DATA_INICIO_SLA']).dt.days
            conditions = [
                df['DATA_INICIO_SLA'].isna(),
                df['TEMPO_RESOLUCAO_SLA'] <= 5,
                df['TEMPO_RESOLUCAO_SLA'] > 5
            ]
            choices = ['Não aplicável', 'Dentro do prazo', 'Fora do prazo']
            df['SLA'] = np.select(conditions, choices, default='Não informado')
            
            # Tempo de resolução geral
            df['TEMPO_RESOLUCAO'] = (df['FECHAMENTO_DT'] - df['ABERTURA_DT']).dt.days
            
            # Extrair palavras-chave
            df = extract_keywords_from_subject(df)
            
            # Extrair empresa principal
            if 'SUBESTIPULANTE' in df.columns:
                df['EMPRESA'] = df['SUBESTIPULANTE'].str.split(' - ').str[0].fillna('Não informado')
                df['CIDADE'] = df['SUBESTIPULANTE'].str.split(' - ').str[1].fillna('Não informado')
            
            # Limpar dados
            df['STATUS'] = df['STATUS'].fillna('Desconhecido')
            df['TIPO ATENDIMENTO'] = df['TIPO ATENDIMENTO'].fillna('Não informado')
            df['CATEGORIA'] = df['CATEGORIA'].fillna('Não informado')
            
            return df
    return None

@st.cache_data
def get_filter_options(df):
    """Retorna todas as opções para os filtros - executado apenas uma vez"""
    options = {
        'status': sorted(df['STATUS'].dropna().unique().tolist()),
        'tipo_atendimento': sorted(df['TIPO ATENDIMENTO'].dropna().unique().tolist()),
        'negocio': sorted(df['NEGÓCIO'].dropna().unique().tolist()) if 'NEGÓCIO' in df.columns else [],
        'empresa': sorted(df['EMPRESA'].dropna().unique().tolist()) if 'EMPRESA' in df.columns else [],
        'categoria': sorted(df['CATEGORIA'].dropna().unique().tolist()) if 'CATEGORIA' in df.columns else [],
        'produto': sorted(df['PRODUTO'].dropna().unique().tolist()) if 'PRODUTO' in df.columns else [],
        'responsavel': sorted(df['RESPONSÁVEL'].dropna().unique().tolist()) if 'RESPONSÁVEL' in df.columns else [],
    }
    return options

def apply_filters(df, filters):
    filtered_df = df.copy()
    
    if filters.get('status') and "TODOS" not in filters['status']:
        filtered_df = filtered_df[filtered_df['STATUS'].isin(filters['status'])]
    if filters.get('tipo_atendimento') and "TODOS" not in filters['tipo_atendimento']:
        filtered_df = filtered_df[filtered_df['TIPO ATENDIMENTO'].isin(filters['tipo_atendimento'])]
    if filters.get('negocio') and "TODOS" not in filters['negocio'] and 'NEGÓCIO' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['NEGÓCIO'].isin(filters['negocio'])]
    if filters.get('empresa') and "TODOS" not in filters['empresa'] and 'EMPRESA' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['EMPRESA'].isin(filters['empresa'])]
    if filters.get('categoria') and "TODOS" not in filters['categoria'] and 'CATEGORIA' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['CATEGORIA'].isin(filters['categoria'])]
    if filters.get('produto') and "TODOS" not in filters['produto'] and 'PRODUTO' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['PRODUTO'].isin(filters['produto'])]
    if filters.get('responsavel') and "TODOS" not in filters['responsavel'] and 'RESPONSÁVEL' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['RESPONSÁVEL'].isin(filters['responsavel'])]
    
    # Filtro de período – CORRIGIDO
    if filters.get('periodo_opcao') == 'PERÍODO PERSONALIZADO':
        data_inicio = filters.get('data_inicio')
        data_fim = filters.get('data_fim')
        if data_inicio is not None and data_fim is not None:
            # Converte para Timestamp e ajusta o fim para o final do dia
            inicio_ts = pd.Timestamp(data_inicio)
            fim_ts = pd.Timestamp(data_fim) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            mask = (filtered_df['ABERTURA_DT'] >= inicio_ts) & (filtered_df['ABERTURA_DT'] <= fim_ts)
            filtered_df = filtered_df[mask]
    
    return filtered_df

def extract_percent_value(metric_str):
    """Extrai o valor percentual de uma string de métrica"""
    try:
        if '(' in metric_str:
            percent_str = metric_str.split('(')[-1].replace('%)', '').replace(')', '')
            return float(percent_str)
    except:
        pass
    return 0.0

def multiselect_with_all(label, options, default_all=True, key=None):
    """Cria um multiselect com a opção 'TODOS'"""
    if not options:
        return []
    options_with_all = ['TODOS'] + options
    if default_all:
        default = ['TODOS']
    else:
        default = options[:3] if len(options) > 3 else options
    selected = st.multiselect(label, options=options_with_all, default=default, key=key)
    if 'TODOS' in selected:
        return ['TODOS']
    return selected

# ============================================
# INTERFACE PRINCIPAL
# ============================================

st.title("📊 Lockton Analytics - Movimentação de Beneficiários")
st.markdown("---")

# Sidebar - Upload e Filtros
with st.sidebar:
    st.header("📁 Upload de Dados")
    uploaded_file = st.file_uploader(
        "Carregue a planilha Excel",
        type=['xlsx', 'xls'],
        help="Faça upload do arquivo LOCKTON - Movimentação de Beneficiários.xlsx"
    )
    
    st.markdown("---")
    st.header("🔍 Filtros")
    
    df = None
    filters = {'periodo_opcao': 'TODAS_AS_DATAS'}
    
    if uploaded_file is not None:
        df = load_and_process_data(uploaded_file)
        
        if df is not None and len(df) > 0:
            options = get_filter_options(df)
            min_date = df['ABERTURA_DT'].min().date()
            max_date = df['ABERTURA_DT'].max().date()
            
            filters['status'] = multiselect_with_all("Status", options['status'], default_all=True, key="status_filter")
            filters['tipo_atendimento'] = multiselect_with_all("Tipo de Atendimento", options['tipo_atendimento'], default_all=True, key="tipo_filter")
            if options['negocio']:
                filters['negocio'] = multiselect_with_all("Negócio", options['negocio'], default_all=True, key="negocio_filter")
            if options['empresa']:
                filters['empresa'] = multiselect_with_all("Empresa (SUBESTIPULANTE)", options['empresa'], default_all=True, key="empresa_filter")
            if options['categoria']:
                filters['categoria'] = multiselect_with_all("Categoria", options['categoria'], default_all=True, key="categoria_filter")
            if options['produto']:
                filters['produto'] = multiselect_with_all("Produto", options['produto'], default_all=True, key="produto_filter")
            if options['responsavel']:
                filters['responsavel'] = multiselect_with_all("Responsável", options['responsavel'], default_all=True, key="responsavel_filter")
            
            st.subheader("📅 Período")
            periodo_opcao = st.radio(
                "Selecione o período:",
                options=["TODAS AS DATAS", "PERÍODO PERSONALIZADO"],
                index=0,
                key="periodo_radio"
            )
            filters['periodo_opcao'] = periodo_opcao
            if periodo_opcao == "PERÍODO PERSONALIZADO":
                col1, col2 = st.columns(2)
                with col1:
                    filters['data_inicio'] = st.date_input("Data Inicial", value=min_date, min_value=min_date, max_value=max_date)
                with col2:
                    filters['data_fim'] = st.date_input("Data Final", value=max_date, min_value=min_date, max_value=max_date)
            
            st.markdown("---")
            if st.button("🔄 Resetar todos os filtros", use_container_width=True):
                st.rerun()
    
    st.markdown("---")
    st.markdown("### 📌 Sobre o App")
    st.markdown("""
    Esta aplicação analisa chamados de atendimento de benefícios da Lockton.
    
    **Funcionalidades:**
    - Performance do atendimento (SLA)
    - Tipos de demanda mais comuns
    - Análise por negócio/unidade
    - Produtos mais demandados
    - Análise de reincidência
    - Filtros interativos com opção "TODOS"
    """)

# ============================================
# CORPO PRINCIPAL DO DASHBOARD
# ============================================
if uploaded_file is not None and df is not None and len(df) > 0:
    filtered_df = apply_filters(df, filters)
    st.info(f"📊 Mostrando {len(filtered_df):,} de {len(df):,} chamados")
    
    # ============================================
    # 1. VISÃO GERAL - KPI CARDS
    # ============================================
    st.subheader("📈 Visão Geral")
    stats = get_summary_stats(filtered_df)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Chamados", stats['Total de Chamados'])
    with col2:
        st.metric("Chamados Abertos", stats['Chamados Abertos'])
    with col3:
        st.metric("Chamados Concluídos", stats['Chamados Concluídos'])
    with col4:
        st.metric("Chamados Cancelados", stats['Chamados Cancelados'])
    
    st.markdown("### ⏱️ Métricas de SLA (Equipe Técnica)")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("✅ Dentro do SLA", stats['Dentro do SLA'])
    with col2:
        st.metric("❌ Fora do SLA", stats['Fora do SLA'])
    with col3:
        st.metric("⏰ Tempo Médio Geral", stats['Tempo Médio Resolução'])
    with col4:
        st.metric("📅 Período", stats['Período Analisado'].split(' a ')[0] if ' a ' in stats['Período Analisado'] else stats['Período Analisado'])
    
    dentro_valor = extract_percent_value(stats['Dentro do SLA'])
    if dentro_valor > 0:
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=dentro_valor,
            title={'text': "Taxa de Conformidade SLA (Técnica)", 'font': {'size': 16}},
            delta={'reference': 90},
            gauge={
                'axis': {'range': [None, 100], 'tickwidth': 1},
                'bar': {'color': "darkblue", 'thickness': 0.3},
                'steps': [
                    {'range': [0, 50], 'color': '#ff4b4b'},
                    {'range': [50, 70], 'color': '#ffa500'},
                    {'range': [70, 90], 'color': '#ffeb3b'},
                    {'range': [90, 100], 'color': '#4caf50'}
                ],
                'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 90}
            }
        ))
        fig_gauge.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20))
        st.plotly_chart(fig_gauge, use_container_width=True)
    
    st.markdown("---")
    
    # ============================================
    # 2. GRÁFICOS PRINCIPAIS
    # ============================================
    col1, col2 = st.columns(2)
    with col1:
        tipo_counts = filtered_df['TIPO ATENDIMENTO'].value_counts().head(10).reset_index()
        tipo_counts.columns = ['Tipo de Atendimento', 'Quantidade']
        fig = px.bar(tipo_counts, x='Quantidade', y='Tipo de Atendimento', orientation='h',
                     title="Top 10 Tipos de Atendimento", color='Quantidade',
                     color_continuous_scale='Blues', text='Quantidade')
        fig.update_traces(textposition='outside')
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        sla_status_chart = pd.crosstab(filtered_df['STATUS'], filtered_df['SLA'])
        fig = px.bar(sla_status_chart, barmode='stack', title="Status dos Chamados vs. Cumprimento de SLA (Técnica)",
                     labels={'value': 'Quantidade', 'variable': 'SLA', 'STATUS': 'Status'},
                     color_discrete_sequence=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        if 'NEGÓCIO' in filtered_df.columns:
            negocio_counts = filtered_df['NEGÓCIO'].value_counts().reset_index()
            negocio_counts.columns = ['Negócio', 'Quantidade']
            negocio_counts = negocio_counts[negocio_counts['Negócio'].notna() & (negocio_counts['Negócio'] != '')]
            if len(negocio_counts) > 0:
                fig = px.pie(negocio_counts, values='Quantidade', names='Negócio',
                             title="Distribuição de Chamados por Negócio", hole=0.3,
                             color_discrete_sequence=px.colors.qualitative.Set3)
                st.plotly_chart(fig, use_container_width=True)
    with col2:
        if 'PRODUTO' in filtered_df.columns:
            produto_counts = filtered_df['PRODUTO'].value_counts().reset_index()
            produto_counts.columns = ['Produto', 'Quantidade']
            produto_counts = produto_counts[produto_counts['Produto'].notna() & (produto_counts['Produto'] != '')]
            if len(produto_counts) > 0:
                fig = px.pie(produto_counts, values='Quantidade', names='Produto',
                             title="Distribuição de Chamados por Produto", hole=0.3,
                             color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # ============================================
    # 3. ANÁLISE DE CATEGORIA
    # ============================================
    if 'CATEGORIA' in filtered_df.columns:
        st.subheader("🔍 Análise de Categoria")
        categoria_counts = filtered_df['CATEGORIA'].value_counts().head(10).reset_index()
        categoria_counts.columns = ['Categoria', 'Quantidade']
        categoria_counts = categoria_counts[categoria_counts['Categoria'].notna() & (categoria_counts['Categoria'] != '')]
        if len(categoria_counts) > 0:
            fig = px.bar(categoria_counts, x='Quantidade', y='Categoria', orientation='h',
                         title="Top 10 Categorias de Movimentação", color='Quantidade',
                         color_continuous_scale='Oranges', text='Quantidade')
            fig.update_traces(textposition='outside')
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")
    
    # ============================================
    # 4. EVOLUÇÃO TEMPORAL
    # ============================================
    st.subheader("📅 Evolução Temporal")
    if len(filtered_df) > 0:
        monthly = filtered_df.groupby('ANO_MES').size().reset_index(name='Quantidade')
        monthly = monthly.sort_values('ANO_MES')
        if len(monthly) > 0:
            fig = px.line(monthly, x='ANO_MES', y='Quantidade', title="Evolução Mensal de Chamados",
                          markers=True, labels={'ANO_MES': 'Mês', 'Quantidade': 'Chamados'})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    st.markdown("---")
    
    # ============================================
    # 5. ANÁLISE DE PERFORMANCE POR RESPONSÁVEL
    # ============================================
    st.subheader("👥 Performance por Responsável")
    if 'RESPONSÁVEL' in filtered_df.columns:
        col1, col2 = st.columns(2)
        with col1:
            resp_stats = get_performance_by_responsavel(filtered_df)
            if len(resp_stats) > 0:
                fig = px.bar(resp_stats, x='Total Chamados', y='Responsável', orientation='h',
                             title="Top Responsáveis por Volume de Chamados", color='Total Chamados',
                             color_continuous_scale='Blues', text='Total Chamados')
                fig.update_traces(textposition='outside')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        with col2:
            equipe_stats = filtered_df.groupby('EQUIPE').agg({
                'PROTOCOLO': 'count',
                'SLA': lambda x: (x == 'Dentro do prazo').mean() * 100
            }).round(1).reset_index()
            equipe_stats.columns = ['Equipe', 'Total Chamados', '% SLA OK']
            equipe_stats = equipe_stats[equipe_stats['Equipe'].notna() & (equipe_stats['Equipe'] != '')]
            equipe_stats = equipe_stats.sort_values('Total Chamados', ascending=False).head(8)
            if len(equipe_stats) > 0:
                fig = px.bar(equipe_stats, x='Total Chamados', y='Equipe', orientation='h',
                             title="Performance por Equipe", color='% SLA OK',
                             color_continuous_scale='RdYlGn', text='Total Chamados', range_color=[0, 100])
                fig.update_traces(textposition='outside')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
    st.markdown("---")
    
    # ============================================
    # 6. ANÁLISE DE PALAVRAS-CHAVE
    # ============================================
    st.subheader("🏷️ Análise de Palavras-Chave nos Assuntos")
    col1, col2 = st.columns(2)
    with col1:
        keyword_counts = filtered_df['PALAVRA_CHAVE'].value_counts().reset_index()
        keyword_counts.columns = ['Palavra-Chave', 'Quantidade']
        keyword_counts = keyword_counts[keyword_counts['Palavra-Chave'] != 'outros']
        if len(keyword_counts) > 0:
            fig = px.pie(keyword_counts, values='Quantidade', names='Palavra-Chave',
                         title="Distribuição de Assuntos por Palavra-Chave", hole=0.3,
                         color_discrete_sequence=px.colors.qualitative.Set3)
            st.plotly_chart(fig, use_container_width=True)
    with col2:
        if len(filtered_df) > 0:
            keyword_monthly = filtered_df.groupby(['ANO_MES', 'PALAVRA_CHAVE']).size().reset_index(name='Quantidade')
            keyword_monthly = keyword_monthly[keyword_monthly['PALAVRA_CHAVE'] != 'outros']
            keyword_monthly = keyword_monthly.sort_values('ANO_MES')
            top_keywords = keyword_counts.head(5)['Palavra-Chave'].tolist() if len(keyword_counts) > 0 else []
            keyword_monthly = keyword_monthly[keyword_monthly['PALAVRA_CHAVE'].isin(top_keywords)]
            if len(keyword_monthly) > 0:
                fig = px.line(keyword_monthly, x='ANO_MES', y='Quantidade', color='PALAVRA_CHAVE',
                              title="Evolução Mensal por Tipo de Assunto", markers=True,
                              labels={'ANO_MES': 'Mês', 'Quantidade': 'Chamados'})
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
    st.markdown("---")
    
    # ============================================
    # 7. ANÁLISE DE REINCIDÊNCIA E GARGALOS
    # ============================================
    st.subheader("🔄 Análise de Reincidência e Gargalos")
    col1, col2 = st.columns(2)
    with col1:
        cpf_analysis = get_reincidencia_analysis(filtered_df)
        if len(cpf_analysis) > 0:
            fig = px.bar(cpf_analysis, x='Quantidade', y='CPF', orientation='h',
                         title="Top Beneficiários com Mais Chamados", color='Quantidade',
                         color_continuous_scale='Purples', text='Quantidade')
            fig.update_traces(textposition='outside')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            st.caption("⚠️ Beneficiários que mais abrem chamados - possível necessidade de treinamento")
    with col2:
        gargalos = get_gargalo_analysis(filtered_df)
        if len(gargalos) > 0:
            fig = px.scatter(gargalos, x='Total Chamados', y='Tempo Médio (dias)', text='Categoria',
                             size='Total Chamados', color='% SLA OK',
                             title="Gargalos: Categorias que Mais Demoram",
                             labels={'Total Chamados': 'Volume de Chamados', 'Tempo Médio (dias)': 'Tempo Médio de Resolução', '% SLA OK': '% SLA OK'},
                             color_continuous_scale='RdYlGn', range_color=[0, 100])
            fig.update_traces(textposition='top center')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    st.markdown("---")
    
    # ============================================
    # 8. ANÁLISES DETALHADAS DE SLA
    # ============================================
    st.header("📊 Análises Detalhadas de SLA")
    
    # 1️⃣ Distribuição Geral do SLA
    sla_dist = filtered_df['SLA'].value_counts().reset_index()
    sla_dist.columns = ['SLA', 'Quantidade']
    fig_donut = px.pie(sla_dist, names='SLA', values='Quantidade', hole=0.55,
                       title="Distribuição Geral do SLA", color='SLA',
                       color_discrete_map={'Dentro do prazo': '#2ecc71', 'Fora do prazo': '#e74c3c',
                                           'Não aplicável': '#95a5a6', 'Não informado': '#f1c40f'})
    fig_donut.update_layout(height=450)
    st.plotly_chart(fig_donut, use_container_width=True)
    
    # 2️⃣ SLA por Status do Chamado
    sla_status = pd.crosstab(filtered_df['STATUS'], filtered_df['SLA'])
    fig_status = px.bar(sla_status, barmode='stack', title="SLA por Status do Chamado",
                        labels={'value': 'Quantidade', 'STATUS': 'Status'},
                        color_discrete_sequence=px.colors.qualitative.Set2)
    fig_status.update_layout(height=450)
    st.plotly_chart(fig_status, use_container_width=True)
    
    # 3️⃣ SLA por Equipe (Percentual)
    sla_equipe = pd.crosstab(filtered_df['EQUIPE'], filtered_df['SLA'], normalize='index') * 100
    sla_equipe = sla_equipe.reset_index().fillna(0)
    for col in ['Dentro do prazo', 'Fora do prazo', 'Não aplicável', 'Não informado']:
        if col not in sla_equipe.columns:
            sla_equipe[col] = 0
    fig_equipe = px.bar(sla_equipe, x='EQUIPE', y=['Dentro do prazo', 'Fora do prazo'],
                        barmode='stack', title="Percentual de SLA por Equipe (%)",
                        labels={'value': 'Percentual (%)', 'variable': 'SLA', 'EQUIPE': 'Equipe'},
                        color_discrete_sequence=['#2ecc71', '#e74c3c'])
    fig_equipe.update_layout(height=450)
    st.plotly_chart(fig_equipe, use_container_width=True)
    
    # 4️⃣ SLA por Responsável (Ranking)
    if 'RESPONSÁVEL' in filtered_df.columns:
        sla_resp = (filtered_df.groupby('RESPONSÁVEL')['SLA']
                    .value_counts(normalize=True).rename('Percentual').reset_index())
        sla_resp = sla_resp[sla_resp['SLA'] == 'Dentro do prazo']
        sla_resp['Percentual'] *= 100
        sla_resp = sla_resp.sort_values('Percentual', ascending=True)
        if len(sla_resp) > 0:
            fig_resp = px.bar(sla_resp, x='Percentual', y='RESPONSÁVEL', orientation='h',
                              title="Ranking de SLA por Responsável (% dentro do prazo)",
                              color='Percentual', color_continuous_scale='Viridis', text='Percentual')
            fig_resp.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            fig_resp.update_layout(height=500)
            st.plotly_chart(fig_resp, use_container_width=True)
    
    # 5️⃣ Evolução Temporal do SLA (mensal)
    sla_time = filtered_df.groupby(['ANO_MES', 'SLA']).size().reset_index(name='Quantidade')
    fig_time = px.line(sla_time, x='ANO_MES', y='Quantidade', color='SLA',
                       title="Evolução Mensal do SLA", markers=True,
                       labels={'ANO_MES': 'Mês', 'Quantidade': 'Quantidade de Chamados', 'SLA': 'SLA'})
    fig_time.update_layout(height=450)
    st.plotly_chart(fig_time, use_container_width=True)
    
    # 6️⃣ Heatmap – SLA por Categoria (%)
    if 'CATEGORIA' in filtered_df.columns:
        heat = pd.crosstab(filtered_df['CATEGORIA'], filtered_df['SLA'], normalize='index') * 100
        heat = heat.fillna(0)
        for col in ['Dentro do prazo', 'Fora do prazo', 'Não aplicável', 'Não informado']:
            if col not in heat.columns:
                heat[col] = 0
        fig_heat = px.imshow(heat, text_auto=".1f", title="Heatmap de SLA por Categoria (%)",
                             labels={'x': 'SLA', 'y': 'Categoria', 'color': '%'},
                             color_continuous_scale='RdYlGn', aspect="auto")
        fig_heat.update_layout(height=500)
        st.plotly_chart(fig_heat, use_container_width=True)
    
    # 7️⃣ SLA por Tipo de Atendimento (%)
    sla_tipo = pd.crosstab(filtered_df['TIPO ATENDIMENTO'], filtered_df['SLA'], normalize='index') * 100
    sla_tipo = sla_tipo.reset_index().fillna(0)
    for col in ['Dentro do prazo', 'Fora do prazo']:
        if col not in sla_tipo.columns:
            sla_tipo[col] = 0
    sla_tipo_melted = sla_tipo.melt(id_vars=['TIPO ATENDIMENTO'],
                                    value_vars=['Dentro do prazo', 'Fora do prazo'],
                                    var_name='SLA_Categoria', value_name='Percentual')
    fig_tipo = px.bar(sla_tipo_melted, x='TIPO ATENDIMENTO', y='Percentual', color='SLA_Categoria',
                      barmode='stack', title="SLA por Tipo de Atendimento (%)",
                      labels={'TIPO ATENDIMENTO': 'Tipo de Atendimento', 'Percentual': 'Percentual (%)'},
                      color_discrete_sequence=['#2ecc71', '#e74c3c'])
    fig_tipo.update_layout(height=450)
    st.plotly_chart(fig_tipo, use_container_width=True)
    
    # 8️⃣ SLA por Produto (Fora do prazo)
    if 'PRODUTO' in filtered_df.columns:
        sla_prod = pd.crosstab(filtered_df['PRODUTO'], filtered_df['SLA'], normalize='index') * 100
        sla_prod = sla_prod.reset_index().fillna(0)
        if 'Fora do prazo' not in sla_prod.columns:
            sla_prod['Fora do prazo'] = 0
        sla_prod = sla_prod.sort_values('Fora do prazo', ascending=False).head(10)
        fig_prod = px.bar(sla_prod, x='PRODUTO', y='Fora do prazo',
                          title="Produtos com Maior % Fora do SLA",
                          labels={'PRODUTO': 'Produto', 'Fora do prazo': '% Fora do SLA'},
                          color='Fora do prazo', color_continuous_scale='Reds', text='Fora do prazo')
        fig_prod.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
        fig_prod.update_layout(height=450)
        st.plotly_chart(fig_prod, use_container_width=True)
    
    # 9️⃣ SLA por Dia da Semana (%)
    dias_ordem = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dias_pt = {'Monday': 'Segunda', 'Tuesday': 'Terça', 'Wednesday': 'Quarta',
               'Thursday': 'Quinta', 'Friday': 'Sexta', 'Saturday': 'Sábado', 'Sunday': 'Domingo'}
    sla_dia = pd.crosstab(filtered_df['DIA_SEMANA'], filtered_df['SLA'], normalize='index') * 100
    sla_dia = sla_dia.reindex(dias_ordem).fillna(0)
    sla_dia.index = sla_dia.index.map(dias_pt)
    fig_dia = px.imshow(sla_dia, text_auto=".1f", title="SLA por Dia da Semana (%)",
                        labels={'x': 'SLA', 'y': 'Dia da Semana', 'color': '%'},
                        color_continuous_scale='RdYlGn', aspect="auto")
    fig_dia.update_layout(height=400)
    st.plotly_chart(fig_dia, use_container_width=True)
    
    # 🔟 Pareto – Onde Estoura o SLA
    pareto = (filtered_df[filtered_df['SLA'] == 'Fora do prazo']
              .groupby('CATEGORIA').size().sort_values(ascending=False).reset_index(name='Qtd').head(10))
    if len(pareto) > 0:
        fig_pareto = px.bar(pareto, x='Qtd', y='CATEGORIA', orientation='h',
                            title="Pareto – Principais Causas de SLA Fora do Prazo",
                            color='Qtd', color_continuous_scale='Reds', text='Qtd')
        fig_pareto.update_traces(textposition='outside')
        fig_pareto.update_layout(height=450)
        st.plotly_chart(fig_pareto, use_container_width=True)
    else:
        st.info("Nenhum chamado fora do prazo para exibir no Pareto.")
    
    st.markdown("---")
    
    # ============================================
    # 9. VISUALIZAÇÃO DE PROTOCOLOS POR SLA
    # ============================================
    st.subheader("🔍 Consultar Protocolos por SLA")
    col1, col2 = st.columns(2)
    with col1:
        sla_filtro = st.multiselect("Filtrar por SLA:",
                                     options=['Dentro do prazo', 'Fora do prazo', 'Não aplicável', 'Não informado'],
                                     default=['Fora do prazo'], key="sla_filter_protocolos")
    with col2:
        status_filtro = st.multiselect("Filtrar por Status:", options=filtered_df['STATUS'].unique().tolist(),
                                       default=[], key="status_filter_protocolos")
    df_protocolos = filtered_df.copy()
    if sla_filtro:
        df_protocolos = df_protocolos[df_protocolos['SLA'].isin(sla_filtro)]
    if status_filtro:
        df_protocolos = df_protocolos[df_protocolos['STATUS'].isin(status_filtro)]
    st.caption(f"📌 **{len(df_protocolos):,} protocolos** encontrados com os filtros selecionados.")
    colunas_protocolo = ['PROTOCOLO', 'STATUS', 'SLA', 'EQUIPE', 'RESPONSÁVEL',
                         'TIPO ATENDIMENTO', 'ABERTURA', 'FECHAMENTO', 'ASSUNTO']
    colunas_existentes = [c for c in colunas_protocolo if c in df_protocolos.columns]
    if len(df_protocolos) > 0:
        st.dataframe(df_protocolos[colunas_existentes], use_container_width=True, height=400)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_protocolos[colunas_existentes].to_excel(writer, index=False, sheet_name='Protocolos')
        excel_data = output.getvalue()
        st.download_button(label="📥 Download dos protocolos filtrados (Excel)", data=excel_data,
                           file_name=f"protocolos_sla_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Nenhum protocolo encontrado com os filtros selecionados.")
    st.markdown("---")
    
    # ============================================
    # 10. TABELA DE DADOS
    # ============================================
    st.subheader("📋 Tabela de Dados Detalhada")
    display_cols = ['PROTOCOLO', 'STATUS', 'SLA', 'EQUIPE', 'RESPONSÁVEL', 'PALAVRA_CHAVE',
                    'TIPO ATENDIMENTO', 'ABERTURA', 'FECHAMENTO', 'TEMPO_RESOLUCAO',
                    'NEGÓCIO', 'CATEGORIA', 'PRODUTO', 'ASSUNTO', 'CPF BENEFICIARIO']
    available_cols = [col for col in display_cols if col in filtered_df.columns]
    if 'TEMPO_RESOLUCAO' in filtered_df.columns:
        filtered_df['DIAS_RESOLUCAO'] = filtered_df['TEMPO_RESOLUCAO'].apply(
            lambda x: f"{x:.0f} dias" if pd.notna(x) else "Não resolvido")
        if 'DIAS_RESOLUCAO' in filtered_df.columns:
            available_cols.append('DIAS_RESOLUCAO')
    page_size = 100
    total_pages = (len(filtered_df) + page_size - 1) // page_size
    if total_pages > 1:
        col1, col2 = st.columns([3, 1])
        with col1:
            page_num = st.number_input("Página", min_value=1, max_value=total_pages, value=1, step=1)
        with col2:
            st.caption(f"Total: {len(filtered_df):,} registros")
        start_idx = (page_num - 1) * page_size
        end_idx = min(start_idx + page_size, len(filtered_df))
        display_df = filtered_df[available_cols].iloc[start_idx:end_idx]
        st.caption(f"Mostrando registros {start_idx + 1} a {end_idx} de {len(filtered_df):,}")
    else:
        display_df = filtered_df[available_cols]
    st.dataframe(display_df, use_container_width=True, height=400)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        filtered_df[available_cols].to_excel(writer, index=False, sheet_name='Dados')
    excel_data = output.getvalue()
    st.download_button(label="📥 Download dos dados filtrados (Excel)", data=excel_data,
                       file_name=f"lockton_dados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    st.markdown("---")
    st.caption(f"📊 Dashboard desenvolvido para análise de chamados Lockton | Total de registros: {len(df):,}")

elif uploaded_file is not None:
    st.error("Erro ao carregar os dados. Verifique se o arquivo está no formato correto.")
else:
    st.info("👈 **Faça o upload do arquivo Excel na barra lateral para começar a análise.**")
    with st.expander("📖 Estrutura esperada da planilha"):
        st.markdown("""
        ### Colunas necessárias:
        - `STATUS` - Situação do chamado (Aberto, Concluído, Cancelado)
        - `PROTOCOLO` - Identificador único do chamado
        - `ABERTURA` - Data/hora de abertura
        - `FECHAMENTO` - Data/hora de fechamento (pode ser vazio)
        - `TIPO ATENDIMENTO` - Tipo da demanda
        - `ASSUNTO` - Descrição do chamado
        - `EQUIPE` - Equipe responsável (Lockton, JBS, etc.) – essencial para cálculo do SLA
        
        ### Colunas opcionais:
        - `NEGÓCIO`, `CATEGORIA`, `PRODUTO`, `SUBESTIPULANTE`, `RESPONSÁVEL`, `SOLICITANTE`, `CPF BENEFICIARIO`
        """)