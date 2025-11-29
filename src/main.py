import sqlite3
import pandas as pd
import os
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns

# ignorar warnings de pandas
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


db_filename = 'database/universidad.db'
data_base= 'universidad'

if os.path.exists(db_filename):
    conn= sqlite3.connect(db_filename)
    cursor= conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    print(f"Conexión a la base de datos {data_base} establecida con éxito.")

    # 1. DML INSERT CURSO
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    curso =('Big Data & AI', 20, 5)
    df_curso = pd.read_sql_query(f"SELECT * FROM cursos WHERE nombre_curso = '{curso[0]}'", conn)

    if len(df_curso):
          print(f"El curso '{curso[0]}' ya existe en la base de datos.")
    else:
        conn.execute ("INSERT INTO cursos (nombre_curso, id_profesor, creditos) VALUES (?,?,?);", curso)
        print(f"Curso {curso[0]} insertado correctamente.")
        conn.commit()
    
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    # 2. DML INSERT ESTUDIANTE
    print('✅ 💻 ---------------------------------------------- ✅ 💻') 
    estudiante =('Lionel', 'Messi','lio@usta.edu.co', '2024-11-25')
    df_estudiante = pd.read_sql_query(f"SELECT * FROM estudiantes WHERE email = '{estudiante[2]}'", conn)

    if len (df_estudiante):
        print(f"El estudiante con email '{estudiante[2]}' ya existe en la base de datos.")
    else:
        conn.execute ("INSERT INTO estudiantes (nombre, apellido, email, fecha_ingreso) VALUES (?,?,?,?);", estudiante) 
        print(f"Estudiante con email '{estudiante[2]}' insertado correctamente.")
        conn.commit()
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    # 3 DML INSERT INSCRIPCION
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    fecha=dt.datetime.now().date()
    inscripcion = (int(df_estudiante['id_estudiante'][0]), int(df_curso['id_curso'][0]), fecha)
    df_inscripcion = pd.read_sql_query(f"""SELECT * FROM inscripciones  where id_estudiante = {inscripcion[0]} and 
                                       id_curso = {inscripcion[1]}""", conn)
    
    if len (df_inscripcion):
        print(f"La inscripción del estudiante '{df_estudiante['nombre'][0]}' en el curso '{df_curso['nombre_curso'][0]}' ya existe en la base de datos.")
    else:
        conn.execute ("INSERT INTO inscripciones (id_estudiante, id_curso, fecha_inscripcion) VALUES (?,?,?);", 
                        inscripcion) 
        print(f"Inscripción del estudiante {df_estudiante['nombre'][0]} en el curso {df_curso['nombre_curso'][0]} insertada correctamente.")
        conn.commit()
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    # 4. Actualizar los creditos curso
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    ajustes=(3, int(df_curso['id_curso'][0]))
    print(f"Créditos del curso {df_curso['nombre_curso'][0]} actualizados correctamente.")
    conn.execute("UPDATE cursos SET creditos = ? WHERE id_curso = ?;", ajustes)
    conn.commit()
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    # 5. Retiro de estudiante
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    id_estudiante_retiro=10
    df_estudiante_retiro = pd.read_sql_query(f"SELECT * FROM estudiantes WHERE id_estudiante = {id_estudiante_retiro}", conn)
    if(len(df_estudiante_retiro)):
        conn.execute("DELETE FROM inscripciones WHERE id_estudiante = ?;", (id_estudiante_retiro,))
        conn.execute("DELETE FROM estudiantes WHERE id_estudiante = ?;", (id_estudiante_retiro,))
        print(f"Estudiante con id {id_estudiante_retiro} retirado correctamente.")
        conn.commit()
    else:
        print(f"El estudiante no existe en la base de datos.")
    
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    # 6. Metricas Agregacion 
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    df_cantidad_estudiantes = pd.read_sql_query("SELECT COUNT(id_estudiante) AS cantidad_estudiantes FROM estudiantes;", conn)
    print(f"Cantidad de estudiantes: {df_cantidad_estudiantes['cantidad_estudiantes'][0]}")

    df_cantidad_cursos = pd.read_sql_query("SELECT COUNT(id_curso) AS cantidad_cursos FROM cursos;", conn)
    print(f"Cantidad de cursos: {df_cantidad_cursos['cantidad_cursos'][0]}")

    df_promedio_creditos = pd.read_sql_query("SELECT AVG(creditos) AS promedio_creditos FROM cursos;", conn)
    print(f"Promedio de créditos: {df_promedio_creditos['promedio_creditos'][0]}")
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    #7. Auditoria Profesores
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    df_profesor_curso= pd.read_sql_query("SELECT p.nombre, c.nombre_curso " \
    "FROM cursos c  RIGHT JOIN profesores p on p.id_profesor=c.id_profesor ", conn)
    print(df_profesor_curso)
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    #8. Popularidad 
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    df_estudiantes_curso = pd.read_sql_query("SELECT c.nombre_curso, COUNT(i.id_estudiante) as cantidad_estudiantes " \
    "FROM inscripciones i INNER JOIN  cursos c ON c.id_curso=i.id_curso " \
    "GROUP BY c.nombre_curso ORDER BY cantidad_estudiantes DESC ", conn)
    print(df_estudiantes_curso)
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    #9. Cursos Masivos 
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    df_estudiantes_curso_20 = pd.read_sql_query("SELECT c.nombre_curso, COUNT(i.id_estudiante) as cantidad_estudiantes " \
    "FROM inscripciones i INNER JOIN  cursos c ON c.id_curso=i.id_curso " \
    "GROUP BY c.nombre_curso HAVING COUNT(i.id_estudiante) > 20 ORDER BY cantidad_estudiantes DESC", conn)
    print(df_estudiantes_curso_20)
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    #10. Estudiantes Curso Bases Datos
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    df_curso_estudiantes =pd.read_sql_query("SELECT e.nombre, e.apellido FROM estudiantes e " \
    "INNER JOIN inscripciones i ON i.id_estudiante= e.id_estudiante " \
    "WHERE i.id_curso in (SELECT id_curso FROM cursos WHERE nombre_curso = 'Bases de Datos') ", conn)

    print(df_curso_estudiantes)
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    
    # Comodin poblar el campo nota_final
    print('✅ 💻 ------------------- Comodin ----------------------- ✅ 💻')
    df_notas_vacias =pd.read_sql_query("SELECT count(id_inscripcion) notas_vacias FROM inscripciones " \
    "WHERE nota_final is null ", conn)
    
    print(df_notas_vacias)
    if (int(df_notas_vacias['notas_vacias'][0])>0):
         conn.execute("UPDATE inscripciones SET nota_final = ROUND(1.0 + (4.0) * (ABS(RANDOM()) / 9223372036854775807.0),1) " \
         " WHERE nota_final is null;")
         conn.commit()
         print("Notas actualizadas en las inscripciones")
    else:
         print("No hay notas por ingresar...")
         
    print('✅ 💻 ------------------- Comodin ----------------------- ✅ 💻')



    #11. Infromacion detallada Sabana de Notas
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    df_notas =pd.read_sql_query("SELECT e.nombre, e.apellido, c.nombre_curso, i.nota_final , p.nombre FROM estudiantes e " \
    "INNER JOIN inscripciones i ON i.id_estudiante= e.id_estudiante " \
    "INNER JOIN cursos c on c.id_curso=i.id_curso " \
    "INNER JOIN profesores p on p.id_profesor =c.id_profesor ", conn)

    print(df_notas)
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    #12 imprimir columnas
    print('✅ 💻 ---------------------------------------------- ✅ 💻') 
    print(df_notas.info())
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    #13 analisis Riesgo nota final < 3
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    condicion =df_notas['nota_final']< 3.0
    df_riesgo =df_notas[condicion]
    print(df_riesgo)
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    #14 Tabla Pivote
    print('✅ 💻 ---------------------------------------------- ✅ 💻')
    tabla_promeido = pd.pivot_table(
         df_notas,
         index='nombre_curso',
         values='nota_final',
         aggfunc='mean'
    )

    print(tabla_promeido)
    print('✅ 💻 ---------------------------------------------- ✅ 💻')

    #15 grafico

    condicion = 'B'

    df_filtrado = df_notas[df_notas['nombre_curso'].str.startswith(condicion)]
    plt.figure(figsize=(10,6))
    sns.barplot(x='nota_final', y='nombre_curso', data=df_filtrado, palette='viridis')

    plt.title('Notas Finales por Curso') # Título del gráfico
    plt.xlabel('Notas Finales')                   # Etiqueta del eje X
    plt.ylabel('Curso con Inicial B')             # Etiqueta del eje Y
    plt.grid(axis='y', linestyle='--', alpha=0.7) # Añade una cuadrícula suave en el eje Y
    plt.show()

    #Finalizar conexion.
    conn.close()


    #print(df_riesgo)












else:
        print(f"Archivo base de datos no encontrado en la ruta {db_filename}")