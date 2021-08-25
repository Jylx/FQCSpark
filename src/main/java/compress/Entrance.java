package compress;


import compress.entities.base_char.Ref_base;
import compress.tar.Tar;
import compress.util.DealReads4;
import compress.util.base_func.ComBase;
import compress.util.DealReads;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class Entrance {


    public static String refFileName = "";
    public static String inputDir = "";
    public static String outputDir0 = "";





    public static void main(String[] args) throws Exception {

        if(args != null && args.length == 4) {
            refFileName = args[0];
            inputDir = args[1];
            outputDir0 = args[2];
            DealReads.PERCENT = Integer.parseInt(args[3]);
        } else {
            throw new Exception("参数个数不是4");
        }


        String outputDir = outputDir0 + File.separator + "compress-output";
        String outputDirA = outputDir + File.separator + "a";
        String outputDirB = outputDir + File.separator + "b";

        long start = System.currentTimeMillis();


        mkdirIfNotExist(outputDir0);
        mkdirIfNotExist(outputDir);
        mkdirIfNotExist(outputDirA);
        mkdirIfNotExist(outputDirB);

        compressFirstAndSecond(refFileName, inputDir, outputDirA);


        compressFourth(inputDir, outputDirB);

        saveProperties(outputDir, DealReads.PERCENT);

        archiveAndBsc(outputDir, outputDir+".target", outputDir+".target.bsc");





        long end = System.currentTimeMillis();
        System.out.println("压缩结束. 共耗费"+(end-start)/1000+"秒");
        System.out.println("压缩结果文件大小：" + new File(outputDir+".target.bsc").length()*1.0/1024/1024 + "MB");
    }



    public static void saveProperties(String out_path_local, int pre) throws IOException {
        File properFile = new File(out_path_local + File.separator + "properties.txt");
        BufferedWriter bw = new BufferedWriter(new FileWriter(properFile));
        bw.write(String.valueOf(pre));
        bw.newLine();
        bw.flush();
        bw.close();
    }





    public static void mkdirIfNotExist(String fileName) {
        File file = new File(fileName);
        if(!file.exists()) {
            file.mkdir();
        }
    }



    public static void compressFirstAndSecond(String refFileName, String inputDir, String outputDir) throws IOException {
        long start = System.currentTimeMillis();


        Ref_base rb = ComBase.createRefBroadcast(refFileName);


        DealReads.setGlobal(inputDir, outputDir);

        int i = 0;
        for (File fp : DealReads.getInput()) {
            DealReads dealReads = new DealReads(rb);
            dealReads.compressFirstAndSecondLinesForOneFastqFile(fp,i);
            i++;
        }

        long end = System.currentTimeMillis();

    }


    public static void compressFourth(String inputDir, String outputDir) throws IOException {

        long start1 = System.currentTimeMillis();

        DealReads4.setGlobal(inputDir, outputDir);

        int i = 0;
        for (File fp : DealReads4.input) {
            DealReads4 dealReads4 = new DealReads4();
            dealReads4.compressFourthLinesForOneFastqFile(fp, i);
            i++;
        }

        long end1 = System.currentTimeMillis();

    }


    public static void archiveAndBsc(String srcDirName, String targetFileName, String bscFileName) throws IOException {

        File srcDirFile = new File(srcDirName);
        File targetFile = new File(targetFileName);
        Tar.archive(srcDirFile, targetFile);

        Tar.bscCompress(targetFileName, bscFileName);

        Tar.deleteFile(srcDirFile);
        Tar.deleteFile(targetFile);

    }

}
