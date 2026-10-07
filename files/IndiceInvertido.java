package com.motorbusqueda;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndiceInvertido {

    public static class IndiceMapper extends Mapper<Object, Text, Text, Text> {
        private Text palabra = new Text();
        private Text documentoId = new Text();
        
        // Stop words básicas para omitir
        private Set<String> stopWords = new HashSet<>(Arrays.asList(
            "el", "la", "los", "las", "un", "una", "unos", "unas", 
            "y", "o", "de", "en", "a", "que", "por", "para", "con", "es", "su", "del", "al"
        ));

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String xmlString = value.toString();
            
            // Extraer el título del documento
            String titulo = "Desconocido";
            Matcher tituloMatcher = Pattern.compile("<title>(.*?)</title>").matcher(xmlString);
            if (tituloMatcher.find()) {
                titulo = tituloMatcher.group(1);
            }
            
            // Extraer el texto
            String texto = "";
            Matcher textoMatcher = Pattern.compile("<text.*?>([\\s\\S]*?)</text>").matcher(xmlString);
            if (textoMatcher.find()) {
                texto = textoMatcher.group(1);
            }
            
            // Normalización obligatoria: minúsculas y eliminación de signos de puntuación
            texto = texto.toLowerCase().replaceAll("[^a-záéíóúñ]", " ");
            
            // Dividir en palabras y usar un Set para el tratamiento de términos repetidos
            String[] palabras = texto.split("\\s+");
            Set<String> palabrasUnicas = new HashSet<>();
            
            for (String p : palabras) {
                if (p.length() > 2 && !stopWords.contains(p)) {
                    palabrasUnicas.add(p);
                }
            }
            
            // Generación de pares clave-valor (término, documento)
            documentoId.set(titulo);
            for (String p : palabrasUnicas) {
                palabra.set(p);
                context.write(palabra, documentoId);
            }
        }
    }

    public static class IndiceReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> documentos = new HashSet<>();
            for (Text val : values) {
                documentos.add(val.toString());
            }
            
            // Formato y almacenamiento del índice final
            context.write(key, new Text(documentos.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: IndiceInvertido <ruta entrada> <ruta salida>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // Definir la lectura de los bloques XML completos
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        Job job = Job.getInstance(conf, "Indice Invertido Wikipedia");
        job.setJarByClass(IndiceInvertido.class);

        job.setMapperClass(IndiceMapper.class);
        job.setReducerClass(IndiceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(XmlInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
