// Importa las dependencias necesarias
const { onObjectFinalized } = require("firebase-functions/v2/storage");
const { initializeApp } = require("firebase-admin/app");
const { getStorage } = require("firebase-admin/storage");
const {
  getFirestore,
  Timestamp,
  FieldValue,
} = require("firebase-admin/firestore");
const { logger } = require("firebase-functions");
const path = require("path");
const os = require("os");
const fs = require("fs").promises;
const fss = require("fs");
const XLSX = require("xlsx");
const {onRequest} = require("firebase-functions/v2/https");

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
    if (
      !contentType ||
      !contentType.includes(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      )
    ) {
      return logger.log("El archivo no es un archivo Excel .xlsx");
    }

    // Evita procesar archivos ya convertidos (si los identificas con un prefijo como "libro_")
    const fileName = path.basename(filePath);
    if (fileName.startsWith("libro_")) {
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

      // Organiza los datos por "Libro" y agrega "Libro" y "Ruta" a cada registro
      const dataByLibro = {};
      jsonData.forEach((row) => {
        const codigoRuta = row["Codigo Ruta Suministro"];
        if (codigoRuta && codigoRuta.length >= 10) {
          const libro = codigoRuta.substring(4, 7); // Obtiene los dígitos del cuarto al séptimo
          const ruta = codigoRuta.substring(7, 13); // Obtiene los últimos 6 dígitos

          if (!dataByLibro[libro]) {
            dataByLibro[libro] = [];
          }

          dataByLibro[libro].push({
            ...row,
            Libro: libro,
            Ruta: ruta,
          });
        }
      });

      // Crea un archivo JSON por cada "Libro"
      for (const [libro, records] of Object.entries(dataByLibro)) {
        const outputFileName = `libro_${libro}.json`;
        const outputFilePath = path.join(os.tmpdir(), outputFileName);

        await fs.writeFile(outputFilePath, JSON.stringify(records, null, 2));
        logger.log(
          `Archivo JSON generado para Libro ${libro}: ${outputFilePath}`
        );

        // Sube el archivo procesado al bucket de Firebase Storage
        const outputStoragePath = path.join(
          path.dirname(filePath),
          outputFileName
        );
        await bucket.upload(outputFilePath, {
          destination: outputStoragePath,
          metadata: { contentType: "application/json" },
        });
        logger.log(`Archivo procesado subido a: ${outputStoragePath}`);

        // Elimina el archivo temporal
        fss.unlinkSync(outputFilePath);
      }

      // Elimina el archivo Excel temporal
      fss.unlinkSync(tempFilePath);
      logger.log("Archivos temporales eliminados.");

      // Elimina el archivo Excel original de storage.
      await bucket.file(filePath).delete();
      logger.log(`Archivo Excel original  ${filePath}  eliminado de storage.`);
    } catch (error) {
      logger.error("Error al procesar el archivo:", error);
    }
  }
);

//

// Función para procesar archivos JSON y guardar en Firestore
exports.processJsonFile = onObjectFinalized(
  {
    timeoutSeconds: 300, // Tiempo máximo de ejecución
    memory: "1GiB", // Memoria asignada
    retry: true, // Habilita reintentos en caso de error
  },
  async (event) => {
    const bucketName = event.data.bucket; // Nombre del bucket
    const fileName = event.data.name; // Nombre del archivo subido

    // Verifica que el archivo sea un JSON
    if (!fileName.endsWith(".json")) {
      logger.log(`Archivo ignorado: ${fileName}`);
      return;
    }

    const bucket = getStorage().bucket(bucketName);
    const tempFilePath = path.join(os.tmpdir(), fileName); // Archivo temporal en /tmp

    try {
      // Descarga el archivo JSON desde Cloud Storage al directorio temporal
      await bucket.file(fileName).download({ destination: tempFilePath });
      logger.log(`Archivo descargado a ${tempFilePath}`);

      // Lee el archivo JSON
      const jsonData = await fs.readFile(tempFilePath, "utf8");
      const data = JSON.parse(jsonData);

      // Determina el nombre de la colección basado en el nombre del archivo
      const collectionName = path.basename(fileName, path.extname(fileName));

      // Referencia a Firestore
      const db = getFirestore();

      // Guarda cada elemento en Firestore
      const batch = db.batch(); // Usa un batch para operaciones atómicas y más rápidas

      data.forEach((item, index) => {
        // Usa un ID único para cada documento
        const docRef = db
          .collection(collectionName)
          .doc(item.id || `doc_${index}`);
        batch.set(docRef, item);
      });

      // Confirma la operación batch
      await batch.commit();
      logger.log(
        `Datos guardados en la colección '${collectionName}' en Firestore correctamente.`
      );

      // Elimina el archivo JSON de storage despues de cargarlo a firestore.
      await bucket.file(fileName).delete();
      logger.log(`Archivo JSON  ${fileName} eliminado de storage.`);
    } catch (error) {
      logger.error("Error procesando el archivo JSON:", error);
      throw error; // Lanza el error para activar reintentos
    } finally {
      // Limpia el archivo temporal
      await fs.unlink(tempFilePath);
      logger.log("Archivo temporal eliminado.");
    }
  }
);

// BUSQUEDA

exports.getLibroCollections = onRequest(async (req, res) => {
  try {
    const firestore = getFirestore();
    const collections = await firestore.listCollections();

    // Filter collections that start with "libro_"
    const libroCollections = collections
      .map((col) => col.id)
      .filter((name) => name.startsWith("libro_"));

    res.json({collections: libroCollections});
  } catch (error) {
    console.error("Error fetching collections:", error);
    res.status(500).json({error: "Internal Server Error"});
  }
});

exports.getDocumentsFromCollection = onRequest(async (req, res) => {
  try {
    const firestore = getFirestore();
    const { collectionName, lastDocId } = req.query;

    if (!collectionName) {
      res.status(400).json({ error: "Collection name is required" });
      return;
    }

    const query = firestore.collection(collectionName).orderBy("__name__").limit(10);
    let snapshot;

    if (lastDocId) {
      const lastDoc = await firestore.collection(collectionName).doc(lastDocId).get();
      if (!lastDoc.exists) {
        res.status(400).json({ error: "Invalid lastDocId" });
        return;
      }
      snapshot = await query.startAfter(lastDoc).get();
    } else {
      snapshot = await query.get();
    }

    const documents = snapshot.docs.map((doc) => ({
      id: doc.id,
      ...doc.data(),
    }));

    res.json({ documents });
  } catch (error) {
    console.error("Error fetching documents:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});


// Endpoint de búsqueda
exports.searchMedidores = onRequest(async (req, res) => {
  try {
      const firestore = getFirestore();
      const { collectionName, searchTerm, lastDocId, searchBy } = req.query;

      if (!collectionName || !searchTerm || !searchBy) {
          res.status(400).json({ error: "Collection name, search term, and searchBy field are required" });
          return;
      }

      const searchTermLower = searchTerm.toLowerCase();

      let query = firestore.collection(collectionName)
          .where(searchBy, '>=', searchTerm.toUpperCase())
          .where(searchBy, '<=', searchTerm.toUpperCase() + '\uf8ff')
          .limit(10);

      let snapshot;
      if (lastDocId) {
          const lastDoc = await firestore.collection(collectionName).doc(lastDocId).get();
          if (!lastDoc.exists) {
              res.status(400).json({ error: "Invalid lastDocId" });
              return;
          }
          query = query.startAfter(lastDoc);
      }
      
      snapshot = await query.get();

      const documents = snapshot.docs.map((doc) => {
          const data = doc.data();
          return {
              id: doc.id,
              // Retornar solo los 3 campos necesarios para la lista
              'Nombre Suministro': data['Nombre Suministro'],
              'Codigo Suministro': data['Codigo Suministro'],
              'Direccion Predio': data['Direccion Predio'],
          };
      });

      res.json({ documents });
  } catch (error) {
      console.error("Error searching documents:", error);
      res.status(500).json({ error: "Internal Server Error" });
  }
});

exports.getDocumentDetail = onRequest(async (req, res) => {
try {
   const firestore = getFirestore();
   const { collectionName, documentId } = req.query;

   if (!collectionName || !documentId) {
       res.status(400).json({ error: "Collection name and document ID are required" });
       return;
   }

   const docRef = firestore.collection(collectionName).doc(documentId);
   const docSnap = await docRef.get();

   if (!docSnap.exists) {
       res.status(404).json({ error: "Document not found" });
       return;
   }

   res.json({ document: { id: docSnap.id, ...docSnap.data() } });

} catch (error) {
   console.error("Error fetching document detail:", error);
   res.status(500).json({ error: "Internal Server Error" });
}
});