<div style="text-align: center; padding: 50px; height: 900px; margin: 10px;">
    <br><br>
    <p style="font-size: 1.2em; font-weight: bold;">CURSO DE ESPECIALIZACIÓN EN IA Y BIG DATA</p>
    <br><br><br>
    <h1 style="color: blue; font-size: 3em;">Trabajo de Investigación:<br>Matriz de Confusión</h1>
    <br>
    <h2 style="font-weight: normal;">Predicción de Accidentes Cerebrovasculares mediante Scikit-learn</h2>
    <br><br><br><br><br>
    <div style="font-size: 1.3em;">
        <p><strong>Autor:</strong> Pablo Palacio Cobo</p>
        <p><strong>Fecha:</strong> 12 de mayo de 2026</p>
    </div>
    <br><br><br>
</div>

<div style="page-break-after: always;"></div>
<div style="page-break-after: always;"></div>

<h1>ÍNDICE:</h1>
<!-- Ctrl + Shift + V -->

<h2><a href="#introduccion">1. Introducción</a></h2>
<ul>
    <li>Contexto del problema</li>
    <li>Objetivos del trabajo</li>
</ul>

<h2><a href="#descripcion">2. Descripción del dataset</a></h2>
<ul>
    <li>Origen y características</li>
    <li>Distribución de clases</li>
</ul>

<h2><a href="#preparacion">3. Preparación de los datos</a></h2>
<ul>
    <li>Limpieza y transformaciones</li>
    <li>Justificación de decisiones (evitar data leakage)</li>
</ul>

<h2><a href="#modelos">4. Modelos de clasificación</a></h2>
<ul>
    <li>Modelos implementados</li>
    <li>Justificación de su elección</li>
</ul>

<h2><a href="#metricas">5. Matriz de confusión y métricas</a></h2>
<ul>
    <li>Matrices obtenidas por modelo</li>
    <li>Análisis de falsos positivos y falsos negativos</li>
</ul>

<h2><a href="#evaluacion">6. Evaluación y comparación de resultados</a></h2>
<ul>
    <li>Comparación entre modelos</li>
    <li>Discusión alineada con el tema elegido</li>
</ul>

<h2><a href="#conclusiones">7. Conclusiones y limitaciones</a></h2>

<h2><a href="#mejoras">8. Líneas de mejora y trabajo futuro</a></h2>

<h2><a href="#referencias">9. Referencias</a></h2>

<h2><a href="#anexo">10. Anexo A – Uso de herramientas de Inteligencia Artificial</a></h2>
<ul>
    <li>Herramienta(s) utilizada(s):</li>
    <li>Finalidad del uso:</li>
    <li>Descripción del uso:</li>
    <li>Prompts empleados:</li>
</ul>

<!-- Salto de pagina -->
<div style="page-break-after: always;"></div>

<h2 id="introduccion">Introducción</a></h2>

<p>El accidente cerebrovascular (ACV) representa una de las emergencias médicas más críticas en la actualidad. Según la Organización Mundial de la Salud (OMS), es la segunda causa de muerte a nivel global, responsable de aproximadamente el 11% del total de defunciones. Debido a que la intervención temprana es el factor determinante para reducir la mortalidad y las secuelas permanentes, la identificación de pacientes de alto riesgo mediante variables clínicas (como la hipertensión, el índice de masa corporal y la edad) es una prioridad en salud pública.

Desde una perspectiva técnica, este problema presenta un desafío común en el análisis de datos médicos: el desbalance de clases. En la mayoría de los registros de salud, el número de personas que sufren un ictus es significativamente menor que el de personas sanas. Esto genera un sesgo en los modelos de Machine Learning que, si no se analiza mediante una matriz de confusión, podrían arrojar una falsa sensación de precisión (Accuracy) mientras ignoran por completo a los pacientes en riesgo.</p>

<p>Este trabajo se estructura bajo los siguientes objetivos técnicos:
<ul>
    <li><b>Implementación de modelos predictivos:</b> Desarrollar y comparar al menos dos algoritmos de clasificación (Regresión Logística y Random Forest) utilizando la librería Scikit-learn para predecir el riesgo de ictus.</li>
    <li><b>Análisis crítico de la Matriz de Confusión:</b> Evaluar el rendimiento de los modelos más allá de la precisión global, centrándose en la identificación de Falsos Negativos, dada su gravedad en el contexto médico.</li>
    <li><b>Tratamiento del desbalance de datos:</b> Aplicar y justificar técnicas de balanceo de pesos (class_weight='balanced') para corregir el sesgo hacia la clase mayoritaria y mejorar la capacidad de detección del modelo.</li>
    <li><b>Validación de métricas:</b> Determinar qué métricas (Precision, Recall o F1-Score) son las más adecuadas para este problema específico, fundamentando la decisión en los resultados obtenidos en las matrices de cada modelo.</li>
</ul>
</p>

<h2 id="descripcion">Descripción del dataset</a></h2>

<p>El dataset que he decidido para usar en este trabajo trata sobre un conjuntos de datos que se utiliza para predecir si es probable que un paciente tenga un accidente cerebrovascular en función de los parámetros de entrada como el género, la edad, varias enfermedades y el estado de tabaquismo. Cada fila en los datos proporciona información relevante sobre el paciente. Ademas el origen del dataset es confidencial y con uso educativo unicamente, a excepcion de uso para investigacion pero acreditando al autor. El dataset se puede descargar desde este enlace: 
<a href="https://www.kaggle.com/datasets/fedesoriano/stroke-prediction-dataset" target="_blank">Stroke Prediction Dataset disponible en Kaggle.</a></p>

<p>Tras analizar y explorar el dataset, he podido comprobar que se trata de un dataset con dos clases stroke (0/1) bastante desbalanceado. En total hay 5110 registros de los cuales hay:
<ul>
    <li><b>Stork 0:</b> 4861.</li>
    <li><b>Stork 1:</b> 249.</li>
</ul>
Ademas este dataset cuenta con un total de 201 valores nulo en la columna "bmi" (indice de masa corporal), al ser un numero muy reducido ante mas de 5000 registros he decidido eliminar esos registros. Con lo cual nos deja con la siguiente proporcion de clases: 
<ul>
    <li><b>Stork 0:</b> 4700.</li>
    <li><b>Stork 1:</b> 209.</li>
</ul>
</p>

<!-- <h4>¿Como solucionamos el desbalanceo de clases?</h4>
<p>Por defecto, los modelos de Scikit-learn asumen que todas las filas del dataset tienen la misma importancia (peso = 1). Si tienes 940 sanos y 42 enfermos, el modelo se "esfuerza" 940 veces más en aprender a identificar sanos que en identificar enfermos.  

Al usar class_weight='balanced', Scikit-learn aplica una fórmula matemática para ajustar automáticamente los pesos de las clases de forma inversamente proporcional a su frecuencia.</p> -->


<!-- ANEXO -->
<h2 id="anexo">Uso de herramientas de Inteligencia Artificial</a></h2>
<ul>
    <li>Herramienta(s) utilizada(s):</li>
    <li>Finalidad del uso:</li>
    <li>Descripción del uso:</li>
    <li>Prompts empleados:</li>
</ul>
