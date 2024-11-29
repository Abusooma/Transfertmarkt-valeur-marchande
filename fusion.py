import pandas as pd
from pathlib import Path
from datetime import datetime
from openpyxl import load_workbook


mois_fr = {
    1: "janvier", 2: "février", 3: "mars", 4: "avril",
    5: "mai", 6: "juin", 7: "juillet", 8: "août",
    9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre"
}

mois_fr_inverse = {v: k for k, v in mois_fr.items()}


def convertir_date(date_obj):
    """
    Convertit une date au format standard.
    """
    if isinstance(date_obj, (datetime, pd.Timestamp)):
        return date_obj

    try:
        jour, mois, annee = date_obj.strip().split(' ')
        mois_num = mois_fr_inverse[mois.lower()]
        date_str = f"{jour} {mois_num} {annee}"
        return datetime.strptime(date_str, '%d %m %Y')
    except (ValueError, KeyError):
        pass

    formats = ['%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d']
    for fmt in formats:
        try:
            return datetime.strptime(date_obj.strip(), fmt)
        except (ValueError, AttributeError):
            continue
    raise ValueError(f"Format inconnu pour la date : {date_obj}")


def formater_date_fr(date_obj):
    """
    Formate une date (datetime) au format '25 août 1996'.
    """
    jour = date_obj.day
    mois = mois_fr[date_obj.month]
    annee = date_obj.year
    return f"{jour} {mois} {annee}"


def fusionner_fichiers_excel(dossier_entree, fichier_sortie):
    dataframes = []

    for fichier in Path(dossier_entree).rglob('*'):
        if fichier.suffix in ['.xls', '.xlsx']:
            try:
                df = pd.read_excel(fichier, usecols=[
                                   'NOM', 'DOB', 'DATE', 'VALEUR', 'NOM2'])

                df['DOB'] = df['DOB'].apply(lambda x: convertir_date(x))
                df['DOB'] = df['DOB'].apply(lambda x: formater_date_fr(x))

                dataframes.append(df)
                print(f"Fichier traité : {fichier.name}")
            except Exception as e:
                print(
                    f"Erreur lors du traitement du fichier {fichier.name}: {e}")

    if not dataframes:
        print("Aucun fichier Excel n'a été trouvé.")
        return

    df_final = pd.concat(dataframes, ignore_index=True)

    if 'Source_Fichier' in df_final.columns:
        df_final.drop(columns=['Source_Fichier'], inplace=True)

    df_final.to_excel(fichier_sortie, index=False, engine='openpyxl')

    workbook = load_workbook(fichier_sortie)
    worksheet = workbook.active

    for col in worksheet.columns:
        max_length = 0
        col_letter = col[0].column_letter
        for cell in col:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        adjusted_width = max_length + 2
        worksheet.column_dimensions[col_letter].width = adjusted_width

    workbook.save(fichier_sortie)

    print(f"\nFichier final créé : {fichier_sortie}")
    print(f"Nombre total de lignes : {len(df_final)}")


if __name__ == "__main__":
    base_dir = Path(__file__).parent
    dossier_source = base_dir / "resultats"
    fichier_de_sortie = "fichier_final.xlsx" # remplace ce fichier par le nom que tu desir
    fusionner_fichiers_excel(dossier_source, fichier_de_sortie)
