from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, sum as spark_sum

# Crea una sessione Spark per gestire le operazioni sui dati
spark = SparkSession.builder \
    .appName("Analisi della Serie A 2023-2024") \
    .getOrCreate()

# Specifica il percorso del file JSON contenente i dati delle partite
json_file_path = "C:\\Users\\gianm\\OneDrive\\Desktop\\it.1.json"

# Carica i dati dal file JSON in un DataFrame
partite_df = spark.read.option("multiLine", "true").json(json_file_path)

# Mostra la struttura del DataFrame per comprendere i tipi di dati e le colonne disponibili
partite_df.printSchema()

# Estrai i dati delle partite in un nuovo DataFrame
# Ogni riga rappresenta una partita con dettagli associati
partite_esplose_df = partite_df.select(explode("matches").alias("partita"))

class AnalisiSquadra:
    def __init__(self, partite_df, nome_squadra):
        """Inizializza l'oggetto AnalisiSquadra con il DataFrame delle partite e il nome della squadra."""
        self.partite_df = partite_df
        self.nome_squadra = nome_squadra

    def vittorie(self):
        """Calcola il numero di vittorie della squadra."""
        return self.partite_df.filter(
            ((col("partita.team1") == self.nome_squadra) & (
                    col("partita.score.ft").getItem(0) > col("partita.score.ft").getItem(1))) |
            ((col("partita.team2") == self.nome_squadra) & (
                    col("partita.score.ft").getItem(0) < col("partita.score.ft").getItem(1)))
        ).count()

    def sconfitte(self):
        """Calcola il numero di sconfitte della squadra."""
        return self.partite_df.filter(
            ((col("partita.team1") == self.nome_squadra) & (
                    col("partita.score.ft").getItem(0) < col("partita.score.ft").getItem(1))) |
            ((col("partita.team2") == self.nome_squadra) & (
                    col("partita.score.ft").getItem(0) > col("partita.score.ft").getItem(1)))
        ).count()

    def pareggi(self):
        """Calcola il numero di pareggi della squadra."""
        return self.partite_df.filter(
            ((col("partita.team1") == self.nome_squadra) & (
                    col("partita.score.ft").getItem(0) == col("partita.score.ft").getItem(1))) |
            ((col("partita.team2") == self.nome_squadra) & (
                    col("partita.score.ft").getItem(0) == col("partita.score.ft").getItem(1)))
        ).count()

    def gol_segnati(self):
        """Calcola il numero totale di gol segnati dalla squadra."""
        gol_home = self.partite_df.filter(col("partita.team1") == self.nome_squadra) \
                         .agg(spark_sum(col("partita.score.ft").getItem(0))).collect()[0][0] or 0

        gol_away = self.partite_df.filter(col("partita.team2") == self.nome_squadra) \
                         .agg(spark_sum(col("partita.score.ft").getItem(1))).collect()[0][0] or 0

        return gol_home + gol_away

    def gol_subiti(self):
        """Calcola il numero totale di gol subiti dalla squadra."""
        gol_subiti_home = self.partite_df.filter(col("partita.team1") == self.nome_squadra) \
                                  .agg(spark_sum(col("partita.score.ft").getItem(1))).collect()[0][0] or 0

        gol_subiti_away = self.partite_df.filter(col("partita.team2") == self.nome_squadra) \
                                  .agg(spark_sum(col("partita.score.ft").getItem(0))).collect()[0][0] or 0

        return gol_subiti_home + gol_subiti_away

    def confronta_con(self, altra_squadra):
        """Confronta le statistiche della squadra attuale con un'altra squadra."""
        confronto = []
        confronto.append(
            f"{self.nome_squadra} e {altra_squadra.nome_squadra} hanno lo stesso numero di Vittorie." if self.vittorie() == altra_squadra.vittorie() else f"{self.nome_squadra} ha {abs(self.vittorie() - altra_squadra.vittorie())} Vittorie in {'più' if self.vittorie() > altra_squadra.vittorie() else 'meno'} rispetto a {altra_squadra.nome_squadra}."
        )
        confronto.append(
            f"{self.nome_squadra} ha {abs(self.sconfitte() - altra_squadra.sconfitte())} Sconfitte in {'più' if self.sconfitte() > altra_squadra.sconfitte() else 'meno'} rispetto a {altra_squadra.nome_squadra}."
        )
        confronto.append(
            f"{self.nome_squadra} ha {abs(self.pareggi() - altra_squadra.pareggi())} Pareggi in {'più' if self.pareggi() > altra_squadra.pareggi() else 'meno'} rispetto a {altra_squadra.nome_squadra}."
        )
        confronto.append(
            f"{altra_squadra.nome_squadra} ha {abs(altra_squadra.gol_segnati() - self.gol_segnati())} Gol segnati in {'più' if altra_squadra.gol_segnati() > self.gol_segnati() else 'meno'} rispetto a {self.nome_squadra}."
        )
        confronto.append(
            f"{altra_squadra.nome_squadra} ha {abs(altra_squadra.gol_subiti() - self.gol_subiti())} Gol subiti in {'più' if altra_squadra.gol_subiti() > self.gol_subiti() else 'meno'} rispetto a {self.nome_squadra}."
        )
        return "\n".join(confronto)

# Filtra le partite per SS Lazio e AS Roma
partite_lazio_df = partite_esplose_df.filter((col("partita.team1") == "SS Lazio") | (col("partita.team2") == "SS Lazio"))
partite_roma_df = partite_esplose_df.filter((col("partita.team1") == "AS Roma") | (col("partita.team2") == "AS Roma"))

# Crea istanze di analisi per Lazio e Roma
analisi_lazio = AnalisiSquadra(partite_lazio_df, "SS Lazio")
analisi_roma = AnalisiSquadra(partite_roma_df, "AS Roma")

# Stampa i risultati delle statistiche
print(f"Lazio: Vittorie: {analisi_lazio.vittorie()}, Sconfitte: {analisi_lazio.sconfitte()}, Pareggi: {analisi_lazio.pareggi()}, Gol segnati: {analisi_lazio.gol_segnati()}, Gol subiti: {analisi_lazio.gol_subiti()}")
print(f"Roma: Vittorie: {analisi_roma.vittorie()}, Sconfitte: {analisi_roma.sconfitte()}, Pareggi: {analisi_roma.pareggi()}, Gol segnati: {analisi_roma.gol_segnati()}, Gol subiti: {analisi_roma.gol_subiti()}")

# Confronto finale tra Lazio e Roma
print("\nConfronto tra Lazio e Roma:")
print(analisi_lazio.confronta_con(analisi_roma))
