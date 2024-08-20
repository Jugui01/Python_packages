import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from scipy.stats import f_oneway
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from io import BytesIO


def summarize_column(df, column_name):
    # Vérifie si la colonne existe dans le DataFrame
    if column_name not in df.columns:
        raise ValueError(f"La colonne '{column_name}' n'existe pas dans le DataFrame.")
    
    column_data = df[column_name]
    
    # Fonction pour les visualisations
    def plot_distribution(data, title, kde=False):
        plt.figure(figsize=(10, 6))
        sns.histplot(data, kde=kde, bins=30, stat="density" if kde else "count")
        plt.title(title, fontsize=16, weight='bold')
        plt.xlabel(column_name, fontsize=14)
        plt.ylabel('Density' if kde else 'Count', fontsize=14)
        plt.grid(True)
        plt.show()

    # Résumé pour les variables catégorielles
    if column_data.dtype == 'object' or pd.api.types.is_categorical_dtype(column_data):
        print(f"Résumé de la variable catégorielle '{column_name}':")
        value_counts = column_data.value_counts()
        percent_counts = column_data.value_counts(normalize=True) * 100
        
        # Concaténation des comptes et des pourcentages dans un DataFrame
        summary_df = pd.DataFrame({'Count': value_counts, 'Percentage': percent_counts})
        summary_df = summary_df.sort_values(by='Count', ascending=False)
        print(summary_df)
        
        # Visualisation de la distribution en pourcentage avec valeurs affichées
        plt.figure(figsize=(10, 6))
        sns.set_theme(style="whitegrid")
        ax = sns.barplot(x=summary_df.index, y='Percentage', data=summary_df, palette="Set2")
        plt.title(f"Distribution de la variable catégorielle '{column_name}' en pourcentage", fontsize=16, weight='bold')
        plt.xlabel(column_name, fontsize=14)
        plt.ylabel('Percentage', fontsize=14)
        plt.grid(True)
        
        # Ajouter les valeurs au-dessus de chaque barre
        for index, row in summary_df.iterrows():
            ax.text(index, row['Percentage'], f'{row["Percentage"]:.1f}%', color='black', ha="center", va="bottom")
        
        plt.show()
        
    # Résumé pour les variables quantitatives
    elif pd.api.types.is_numeric_dtype(column_data):
        print(f"Résumé de la variable quantitative '{column_name}':")
        stats_summary = column_data.describe()
        mean = stats_summary['mean']
        variance = column_data.var()
        
        print(f"Moyenne: {mean}")
        print(f"Variance: {variance}")
        
        # Ajustement de plusieurs distributions
        distributions = [stats.norm, stats.expon, stats.lognorm, stats.gamma, stats.beta]
        best_fit_name, best_fit_params, best_ks_stat = None, None, np.inf
        
        for distribution in distributions:
            params = distribution.fit(column_data)
            ks_stat, p_value = stats.kstest(column_data, distribution.name, args=params)
            if ks_stat < best_ks_stat:
                best_fit_name = distribution.name
                best_fit_params = params
                best_ks_stat = ks_stat
        
        print(f"Meilleure loi de probabilité: {best_fit_name}")
        print(f"Paramètres: {best_fit_params}")
        
        # Visualisation de la distribution avec KDE
        sns.set_theme(style="darkgrid")
        plot_distribution(column_data, f"Distribution de la variable '{column_name}'\nMoyenne: {mean:.2f}, Variance: {variance:.2f}", kde=True)
        
        # Estimation de la loi de probabilité associée
        plt.figure(figsize=(10, 6))
        sns.histplot(column_data, kde=False, bins=30, stat="density", color='skyblue', edgecolor='black')
        
        # Traçage de la distribution ajustée
        x = np.linspace(column_data.min(), column_data.max(), 100)
        if best_fit_name == "beta":
            a, b, loc, scale = best_fit_params
            y = stats.beta.pdf(x, a, b, loc=loc, scale=scale)
        elif best_fit_name == "gamma":
            a, loc, scale = best_fit_params
            y = stats.gamma.pdf(x, a, loc=loc, scale=scale)
        elif best_fit_name == "lognorm":
            s, loc, scale = best_fit_params
            y = stats.lognorm.pdf(x, s, loc=loc, scale=scale)
        else:
            y = getattr(stats, best_fit_name).pdf(x, *best_fit_params)
        
        plt.plot(x, y, 'r', linewidth=2, label=best_fit_name)
        plt.title(f"Estimation de la distribution pour '{column_name}'\nMoyenne: {mean:.2f}, Variance: {variance:.2f}\nLoi: {best_fit_name} (KS Stat: {best_ks_stat:.4f})", fontsize=16, weight='bold')
        
        plt.xlabel(column_name, fontsize=14)
        plt.ylabel('Density', fontsize=14)
        plt.legend()
        plt.grid(True)
        plt.show()
        
    else:
        print(f"La variable '{column_name}' n'est ni catégorielle ni quantitative.")

def analyze_two_columns(df, column1, column2):
    # Vérifie si les colonnes existent dans le DataFrame
    if column1 not in df.columns or column2 not in df.columns:
        raise ValueError(f"Les colonnes '{column1}' et/ou '{column2}' n'existent pas dans le DataFrame.")
    
    data1 = df[column1]
    data2 = df[column2]

    def plot_distribution(data, title, column_name, kde=False):
        plt.figure(figsize=(10, 6))
        sns.histplot(data, kde=kde, bins=30, stat="density" if kde else "count")
        plt.title(title, fontsize=16, weight='bold')
        plt.xlabel(column_name, fontsize=14)
        plt.ylabel('Density' if kde else 'Count', fontsize=14)
        plt.grid(True)
        plt.show()
    
    summarize_column(df, column1)
    summarize_column(df, column2)

    # Analyse croisée
    if pd.api.types.is_numeric_dtype(data1) and pd.api.types.is_numeric_dtype(data2):
        # Les deux variables sont quantitatives
        correlation, _ = stats.pearsonr(data1, data2)
        print(f"Corrélation de Pearson entre '{column1}' et '{column2}': {correlation:.4f}")
        
        plt.figure(figsize=(10, 6))
        sns.set_theme(style="darkgrid")
        sns.scatterplot(x=data1, y=data2)
        plt.title(f"Scatterplot de '{column1}' vs '{column2}'\nCorrélation de Pearson: {correlation:.4f}", fontsize=16, weight='bold')
        plt.xlabel(column1, fontsize=14)
        plt.ylabel(column2, fontsize=14)
        plt.grid(True)
        plt.show()
    
    elif pd.api.types.is_categorical_dtype(data1) and pd.api.types.is_numeric_dtype(data2):
        # Variable 1 catégorielle, variable 2 quantitative
        categories = data1.unique()
        means = data2.groupby(data1).mean()
        variances = data2.groupby(data1).var()
        
        summary_df = pd.DataFrame({'Mean': means, 'Variance': variances})
        print(summary_df)
        
        plt.figure(figsize=(10, 6))
        sns.set_theme(style="whitegrid")
        sns.barplot(x=categories, y=means, palette="Set2")
        plt.title(f"Moyenne de '{column2}' pour chaque catégorie de '{column1}'", fontsize=16, weight='bold')
        plt.xlabel(column1, fontsize=14)
        plt.ylabel(f'Mean {column2}', fontsize=14)
        plt.grid(True)
        plt.show()
        
        # ANOVA
        anova_data = [data2[data1 == category] for category in categories]
        anova_result = f_oneway(*anova_data)
        print(f"ANOVA F-statistic: {anova_result.statistic:.4f}, p-value: {anova_result.pvalue:.4f}")
    
    elif pd.api.types.is_numeric_dtype(data1) and pd.api.types.is_categorical_dtype(data2):
        # Variable 1 quantitative, variable 2 catégorielle
        categories = data2.unique()
        means = data1.groupby(data2).mean()
        variances = data1.groupby(data2).var()
        
        summary_df = pd.DataFrame({'Mean': means, 'Variance': variances})
        print(summary_df)
        
        plt.figure(figsize=(10, 6))
        sns.set_theme(style="whitegrid")
        sns.barplot(x=categories, y=means, palette="Set2")
        plt.title(f"Moyenne de '{column1}' pour chaque catégorie de '{column2}'", fontsize=16, weight='bold')
        plt.xlabel(column2, fontsize=14)
        plt.ylabel(f'Mean {column1}', fontsize=14)
        plt.grid(True)
        plt.show()
        
        # ANOVA
        anova_data = [data1[data2 == category] for category in categories]
        anova_result = f_oneway(*anova_data)
        print(f"ANOVA F-statistic: {anova_result.statistic:.4f}, p-value: {anova_result.pvalue:.4f}")
    
    elif pd.api.types.is_categorical_dtype(data1) and pd.api.types.is_categorical_dtype(data2):
        # Les deux variables sont catégorielles
        confusion_matrix = pd.crosstab(data1, data2, normalize='index') * 100
        
        plt.figure(figsize=(10, 6))
        sns.set_theme(style="whitegrid")
        sns.heatmap(confusion_matrix, annot=True, fmt=".1f", cmap="Reds", cbar=True)
        plt.title(f"Matrice de confusion de '{column1}' et '{column2}' en pourcentage", fontsize=16, weight='bold')
        plt.xlabel(column2, fontsize=14)
        plt.ylabel(column1, fontsize=14)
        plt.grid(True)
        plt.show()
    else:
        print("Aucune analyse croisée possible pour les types de colonnes données.")