
// Importa las dependencias necesarias
const { onObjectFinalized } = require("firebase-functions/v2/storage");
const { initializeApp } = require("firebase-admin/app");
const { getStorage } = require("firebase-admin/storage");
const { getFirestore, Timestamp, FieldValue } = require('firebase-admin/firestore');
const logger = require("firebase-functions/logger");
const path = require("path");
const os = require("os");
const fs = require("fs").promises;
const fss = require("fs");
const XLSX = require("xlsx");

// Inicializa Firebase Admin
initializeApp();

// Función para procesar archivos Excel
exports.processExcelFile = onObjectFinalized(
  {
    memory: "1GiB", // Configura memoria para manejar archivos grandes
    timeoutSeconds: 300, // Configura un tiempo de espera extendido
  },
  async (event) => {
    const fileBucket = event.data.bucket; // Nombre del bucket
    const filePath = event.data.name; // Ruta del archivo en el bucket
    const contentType = event.data.contentType; // Tipo de contenido del archivo

    // Verifica si el archivo es un Excel
    if (!contentType || !contentType.includes("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")) {
      return logger.log("El archivo no es un archivo Excel .xlsx");
    }

    // Evita procesar archivos ya convertidos (si los identificas con un prefijo como "processed_")
    const fileName = path.basename(filePath);
    if (fileName.startsWith("processed_")) {
      return logger.log("El archivo ya fue procesado.");
    }

    try {
      // Descarga el archivo desde Firebase Storage a un directorio temporal
      const bucket = getStorage().bucket(fileBucket);
      const tempFilePath = path.join(os.tmpdir(), fileName);
      await bucket.file(filePath).download({ destination: tempFilePath });
      logger.log(`Archivo descargado a: ${tempFilePath}`);

      // Procesa el archivo Excel
      const workbook = XLSX.readFile(tempFilePath);
      const sheetName = workbook.SheetNames[0]; // Obtiene el nombre de la primera hoja
      const worksheet = workbook.Sheets[sheetName];

      // Convierte los datos de la hoja a JSON
      const jsonData = XLSX.utils.sheet_to_json(worksheet);
      logger.log("Datos procesados del archivo Excel.");

      // Genera un archivo JSON o CSV más liviano
      const outputFileName = `processed_${path.basename(fileName, ".xlsx")}.json`;
      const outputFilePath = path.join(os.tmpdir(), outputFileName);
      fss.writeFileSync(outputFilePath, JSON.stringify(jsonData, null, 2));
      logger.log(`Archivo JSON generado: ${outputFilePath}`);

      // Sube el archivo procesado al bucket de Firebase Storage
      const outputStoragePath = path.join(path.dirname(filePath), outputFileName);
      await bucket.upload(outputFilePath, {
        destination: outputStoragePath,
        metadata: { contentType: "application/json" },
      });
      logger.log(`Archivo procesado subido a: ${outputStoragePath}`);

      // Elimina archivos temporales
      fss.unlinkSync(tempFilePath);
      fss.unlinkSync(outputFilePath);
      logger.log("Archivos temporales eliminados.");

    } catch (error) {
      logger.error("Error al procesar el archivo:", error);
    }
  }
);


//

exports.processJsonFile = onObjectFinalized({
  timeoutSeconds: 540, // Tiempo máximo de ejecución
  memory: "2GiB",      // Memoria asignada
}, async (event) => {
  const bucketName = event.data.bucket; // Nombre del bucket
  const fileName = event.data.name;    // Nombre del archivo subido

  // Verifica que el archivo sea un JSON
  if (!fileName.endsWith(".json")) {
    console.log(`Archivo ignorado: ${fileName}`);
    return;
  }

  const bucket = getStorage().bucket(bucketName);
  const tempFilePath = path.join("/tmp", fileName); // Archivo temporal en /tmp

  try {
    // Descarga el archivo JSON desde Cloud Storage al directorio temporal
    await bucket.file(fileName).download({ destination: tempFilePath });
    console.log(`Archivo descargado a ${tempFilePath}`);

    // Lee el archivo JSON
    const jsonData = await fs.readFile(tempFilePath, "utf8");
    const data = JSON.parse(jsonData);

    // Referencia a Firestore
    const db = getFirestore();

    // Guarda cada elemento en Firestore
    const batch = db.batch(); // Usa un batch para operaciones atómicas y más rápidas
    const collectionName = "data"; // Cambia este nombre por el que necesites

    data.forEach((item, index) => {
      // Usa un ID único para cada documento
      const docRef = db.collection(collectionName).doc(item.id || `doc_${index}`);
      batch.set(docRef, item);
    });

    // Confirma la operación batch
    await batch.commit();
    console.log("Datos guardados en Firestore correctamente.");

  } catch (error) {
    console.error("Error procesando el archivo JSON:", error);
  } finally {
    // Limpia el archivo temporal
    await fs.unlink(tempFilePath);
    console.log("Archivo temporal eliminado.");
  }
});
