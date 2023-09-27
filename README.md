# Description des Données

Ce dataset contient des informations sur les transports publics, notamment les champs suivants :

- **Date :** La date à laquelle l'enregistrement a été effectué.
- **TransportType :** Le type de transport, tel que bus, tram, métro, etc.
- **Route :** La route spécifique ou le numéro de ligne associé à l'enregistrement.
- **DepartureTime :** L'heure de départ prévue du véhicule.
- **ArrivalTime :** L'heure d'arrivée prévue du véhicule.
- **Passagers :** Le nombre de passagers à bord du véhicule.
- **DepartureStation :** La station de départ du véhicule.
- **ArrivalStation :** La station d'arrivée du véhicule.
- **Delay :** Le retard par rapport à l'heure d'arrivée prévue.

# Transformations

Les transformations suivantes ont été appliquées aux données brutes :

- Ajout des colonnes "Year", "Month" et "Day".
- Calcul de la durée de chaque voyage en soustrayant l'heure de départ de l'heure d'arrivée.
- Catégorisation des retards en groupes tels que 'Pas de Retard', 'Retard Court' (1-10 minutes), 'Retard Moyen' (11-20 minutes) et 'Long Retard' (>20 minutes).
- Identification des heures de pointe et hors pointe en fonction du nombre de passagers.
- Calcul du retard moyen, du nombre moyen de passagers et du nombre total de voyages pour chaque itinéraire.

# Lignage des Données

Les données utilisées dans ce projet proviennent de la société TRANS et ont été traitées à l'aide d'Azure Databricks.

# Directives d'Utilisation

## Cas d'Utilisation Potentiels

- **Analyse des Performances de Transport :** Explorez les retards, la durée des voyages et les itinéraires pour évaluer et améliorer les performances du transport public.

- **Optimisation des Itinéraires :** Utilisez les statistiques d'itinéraire pour identifier des opportunités d'optimisation des itinéraires et de planification.

- **Planification des Heures de Pointe :** Identifiez les heures de pointe en fonction du nombre de passagers et des retards pour gérer efficacement les ressources.

- **Suivi des Tendances des Passagers :** Analysez les tendances des passagers pour ajuster les services de transport en fonction de la demande.

## Précautions Importantes

- Assurez-vous de bien comprendre la signification des catégories de retard (Retard Court, Retard Moyen, Long Retard, Pas de Retard) pour interpréter correctement les données liées aux retards.

- Les données peuvent être soumises à des mises à jour périodiques. Avant toute utilisation, assurez-vous de vérifier si les données sont à jour en fonction des besoins spécifiques de votre cas d'utilisation.
