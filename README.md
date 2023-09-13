# ai-masters-bigdata

[RU] Краткое описание проектов:

1. В этой домашней работе вы практикуетесь с Hadoop Streaming, а именно - инференс модели на основе sklearn на кластере. При таком подходе модель обучается на (относительно) небольшом семпле, а инференс может осуществляться параллельно на кластере на датасете любого размера.
В этом задании требуется не просто обучить модель и сделать предсказания, а написать скрипты, для которых пути к датасетам и сама модель будут являться аргументами. Это позволяет легче выводить модели в прод.
2. В этой домашке мы работаем с теми же самыми данными, что и в первом домашнем задании, и задача для обучения модели стоит точно такая же. Прочтите описание данных и задачи ниже.
Однако мы будем работать с данными используя только Hive.
За основу взят датасет и соревнование Criteo Display Advertising challenge
3. В этом задании вам предстоит реализовать алгоритм поиска кратчайшего пути в графе, используя алгоритм Breadth-first search. Алгоритм BFS, в отличие от алгоритма Depth-first search, исследует все возможные пути от целевой вершины одновременно.
4. В этом задании вам предстоит предсказать оценку товара по его текстовому обзору (review). Тренировка модели и предсказание должны быть сделаны с использованием Spark ML.
5. В этом задании вы обучаете модель sklearn и сохраняете ее и делаете предсказания с помощью MLFlow. Обратите внимание, в задании используется MLflow версии 1.30. Все выполняется только на логин-ноде. Кластер не используется.
6. В этом задании вам предстоит предсказать "настроение" (sentiment) обзора товара (review).
В этой работе вы воспользуетесь планировщиком Airflow, чтобы реализовать DAG из нескольких задач:
- предобработка тренировочного датасета на Spark (feature engineering)
- обучение модели sklearn на этих данных
- предобработка тестового датасета на Spark (feature engineering)
- предсказание на тестовом датасете с помощью Spark и pandas_udf, используя предобученную модель sklearn
