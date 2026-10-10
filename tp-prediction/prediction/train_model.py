import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

FEATURES = ["age", "anciennete", "nombre_commandes",
            "montant_total", "montant_moyen", "jours_depuis_derniere_commande"]

rng = np.random.default_rng(42)
N = 5000

nb = rng.integers(0, 31, N)
moyen = np.where(nb == 0, 0, rng.uniform(10, 300, N)).round(2)
df = pd.DataFrame({
    "age": rng.integers(18, 71, N),
    "anciennete": rng.integers(0, 11, N),
    "nombre_commandes": nb,
    "montant_total": (nb * moyen).round(2),
    "montant_moyen": moyen,
    "jours_depuis_derniere_commande": rng.integers(0, 366, N),
})

# Règle cachée : plus on commande et plus on est récent, plus on recommande
score = (0.12 * df["nombre_commandes"] - 0.015 * df["jours_depuis_derniere_commande"]
         + 0.15 * df["anciennete"] - 0.5 + rng.normal(0, 0.8, N))
proba = 1 / (1 + np.exp(-score))
y = (rng.random(N) < proba).astype(int)

X_train, X_test, y_train, y_test = train_test_split(df[FEATURES], y, test_size=0.2, random_state=42)
model = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42)
model.fit(X_train, y_train)
print("Accuracy test :", round(accuracy_score(y_test, model.predict(X_test)), 3))

joblib.dump(model, "modele_commande.pkl")
print("Modèle sauvegardé : modele_commande.pkl")