from ultralytics import YOLO
import cv2
import os

def main():
    # 1. Cargar el modelo base pre-entrenado
    # 'n' es nano (rápido), 'seg' habilita la segmentación (máscaras)
    print("Cargando modelo...")
    model = YOLO("yolo11n-seg.pt")

    # 2. Entrenar el modelo (Afinar)
    # Asegúrate de que la ruta al data.yaml sea absoluta o correcta relativa
    print("Iniciando entrenamiento...")
    results = model.train(
        data="/home/ciabd14/Escritorio/CE_IABD/MIA/UNIDAD3/ActividadEtiquetadoSegmentacion/content/datasets/dataset_muñecos/data.yaml",  # Ruta a tu dataset
        epochs=10,  # Reducido a 10 para la clase (para que dé tiempo)
        imgsz=640,  # Tamaño de imagen
        plots=True,  # Generar gráficas de pérdida
        device='cpu',  # Forzar CPU si no tienen CUDA configurado (opcional)
        project='clase_yolo',  # Carpeta donde se guardan resultados
        name='muñeco_run'  # Nombre del experimento
    )

    # 3. Cargar el modelo que acabamos de entrenar (el mejor resultado)
    # La ruta dependerá de dónde se creó la carpeta 'clase_yolo'
    best_weight_path = os.path.join('clase_yolo', 'muñeco_run', 'weights', 'best.pt')
    print(f"Cargando el modelo entrenado desde: {best_weight_path}")
    tuned_model = YOLO(best_weight_path)

    # 4. Inferencia (Predicción) en nuevas imágenes
    # Usamos una imagen de prueba del dataset
    test_img_path = "/home/ciabd14/Escritorio/CE_IABD/MIA/UNIDAD3/ActividadEtiquetadoSegmentacion/content/datasets/dataset_muñecos/images/val/1c2efa92-IMG_6499.jpeg"  # CAMBIAR POR UNA IMAGEN REAL

    if os.path.exists(test_img_path):
        results = tuned_model(test_img_path)

        # 5. Mostrar resultados
        for result in results:
            # Guardar la imagen en disco
            result.save(filename="resultado_prediccion.jpg")

            # Mostrar en una ventana emergente (típico de OpenCV)
            im_array = result.plot()  # plot() dibuja las máscaras y cajas en la imagen
            cv2.imshow("Deteccion de muñeco", im_array)
            cv2.waitKey(0)  # Esperar a que se pulse una tecla
            cv2.destroyAllWindows()

    else:
        print(f"No se encontró la imagen de prueba: {test_img_path}")


if __name__ == '__main__':
    # En Windows es necesario proteger el entry point para multiproceso
    main()