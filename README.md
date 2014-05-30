hadoop-mr
=========

Repo de code map reduce pour Hadoop YARN qui s'execute sur les logs Wikipedia http://dumps.wikimedia.org/other/pagecounts-raw/

Pour lancer le l'application sur Hadoop 2.2, installez Gradle 1.2 puis executez la commande suivante : 

    cd /home/usr/mon_dossier_de_travail/hadoop-mr/
    gradle build

Ensuite, une archive jar a été produite dans la racine du dossier hadoop-mr : wikipedia.jar

Utilisez votre cluster Hadoop avec la commande suivante : 

    hadoop jar wikipedia.jar

Plusieurs options sont possibles : 

    -in : <Optionel> le dossier d'entrée (sur HDFS), contenant les fichiers à analyser. Par défault "wikipedia".
    -out : <Optionel> le dossier de sortie (sur HDFS), qui contientdra les parties de la phase reduce. Par default "wikipedia-out".
    -date [yyyyMMdd] : <Optionel> précise la date à laquelle l'analyse commence (ex : 20140514)
    -searchTopicFile : précise le fichier contenant la liste des topics à rechercher. Si l'option n'existe pas, la recherche prend par default tous les sujets de wikipedia.
    -restrictionFile : précise le fichier contenant la liste des topics à ignorer (comme la page d'accueil de Wikipédia). Si l'option n'est pas soumise, la recherche ne sera pas restreinte.
                      
