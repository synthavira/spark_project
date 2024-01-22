package com.spark.FileTransfer.data;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class UnzipMoveFile {

    public static void main(String[] args) {
    	System.setProperty("hadoop.home.dir", "//home//synthavi//hadoop");
    	String inputDirectory = "hdfs://localhost:9000/input";
        String outputDirectory = "hdfs://localhost:9000/output";
        String kafkaBootstrapServers = "localhost:9092";
        String kafkaTopic = "file_metadata";
        System.out.println("**************************************Calling");
        SparkConf conf = new SparkConf().setAppName("UnzipAndProcessFiles").set("spark.rpc.message.maxSize", "512");;
        SparkSession sparkSession = SparkSession.builder().config(conf).master("local[*]").getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> zipFilesRDD = sparkContext.textFile(inputDirectory);
        System.out.println("**************************************"+ zipFilesRDD.name());
        zipFilesRDD.foreach(zipFilePath -> {
            processAndMoveUnzippedFiles(zipFilePath, outputDirectory, kafkaBootstrapServers, kafkaTopic);
        });

        sparkContext.stop();
    }

    private static void processAndMoveUnzippedFiles(String zipFilePath, String outputDirectory, String kafkaBootstrapServers, String kafkaTopic) {
        try {
        	System.out.println("************************************* processing");
            Configuration configuration = new Configuration();
            Path path = new Path(zipFilePath);
            FileSystem fileSystem = FileSystem.get(path.toUri(), configuration);
            
           if (!fileSystem.exists(path)) {
                System.err.println("Error: fileS- " + zipFilePath);
                return;
            }System.out.println("************************************* zipfilePath "+zipFilePath);

            String outputDirPath = outputDirectory + "/" + FilenameUtils.getBaseName(zipFilePath);
            File outputDir = new File(outputDirPath);

            /*if (!outputDir.exists()) {
                outputDir.mkdirs();
            }*/

            try (InputStream inputStream = fileSystem.open(path)) {
            	System.out.println("**************************************Calling unzip");
                unzipFiles(inputStream, outputDir);
            }

            String metadata = processFiles(outputDir);
            sendMetadataToKafka(metadata, kafkaBootstrapServers, kafkaTopic);
            moveFiles(outputDir, outputDirectory);

            System.out.println("Processing and moving complete for file: " + zipFilePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void unzipFiles(InputStream inputStream, File outputDir) throws IOException {
        try (ZipInputStream zipInputStream = new ZipInputStream(inputStream)) {
            ZipEntry entry;
            System.out.print("**********************"+zipInputStream.getNextEntry().getName());
            while ((entry = zipInputStream.getNextEntry()) != null) {
                String entryName = entry.getName();
                
                // Check for null characters in the entry name
                if (entryName != null && entryName.indexOf('\0') != -1) {
                    System.err.println("Skipping entry with null character: " + entryName);
                    continue;
                }

                String entryPath = outputDir.getPath() + File.separator + entryName;
                File entryFile = new File(entryPath);

                if (entry.isDirectory()) {
                    entryFile.mkdirs();
                } else {
                    try (OutputStream outputStream = new FileOutputStream(entryFile)) {
                        byte[] buffer = new byte[1024];
                        int bytesRead;

                        while ((bytesRead = zipInputStream.read(buffer)) > 0) {
                            outputStream.write(buffer, 0, bytesRead);
                        }
                    }
                }

                zipInputStream.closeEntry();
            }
        }
    }



    private static String processFiles(File inputDir) {
       
        String extension = "txt";
        long fileSize = 1024L;
        int numPages = 5;

        return String.format("{\"extension\":\"%s\", \"numPages\":%d, \"fileSize\":%d}", extension, numPages, fileSize);
    }

    private static void sendMetadataToKafka(String metadata, String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, metadata));
        }
    }

    private static void moveFiles(File inputDir, String outputDirectory) throws IOException {
        File[] files = inputDir.listFiles();

        if (files != null) {
            for (File file : files) {
                File destFile = new File(outputDirectory + "/" + file.getName());
                FileUtils.moveFile(file, destFile);
            }
        }
    }
}
