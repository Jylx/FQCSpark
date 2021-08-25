package compress.util;


import compress.entities.base_char.MatchEntry;
import compress.entities.base_char.Bases_Seq;
import compress.entities.base_char.Ref_base;
import compress.util.base_func.ComBase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import scala.Tuple2;

import java.io.*;
import java.util.*;


public class DealReads {





    private Ref_base rb;

    public static File[] input;
    public static File[] getInput() {
        return input;
    }


    private static File output ;
    private static BufferedWriter[] bw;


    public static void setGlobal(/*Ref_base _rb, */String inputDirName, String outputDir) throws IOException {
        output = new File(outputDir);
        if(!output.exists()) {
            output.mkdir();
        }

        File tmp = new File(inputDirName);
        if(tmp.isDirectory()){
            input = tmp.listFiles();
        }

        bw = new BufferedWriter[input.length];
        for(int i = 0; i < input.length; i++) {
            String inputFileName = input[i].getAbsolutePath();
            bw[i] = new BufferedWriter(new FileWriter(output + File.separator
             + i + "-" + "a" + "-" + inputFileName.substring(inputFileName.lastIndexOf(File.separator) + 1)
             + ".compress"));
        }


    }


    private int linelen = 0;
    public void addLineLen() {
        linelen++;
    }


    private String preName = "";
    private int preNameNum = 0;

    private String preName1 = "";
    private int preNameNum1 = 0;

    private int baseNum0 = 0;
    private int preNum0 = 0;
    private int preNum0ChaFenNum = 0;

    private int preNum1 = 0;
    private int preNum1Num = 0;

    private int preNum2 = 0;
    private int preNum2Num = 0;

    private int preNum3 = 0;

    private int preNum4 = 0;

    private int preNum5 = 0;
    private int preNum5Num = 0;

    private List<String> nameList = new ArrayList<>();
    private List<String> name1List = new ArrayList<>();

    private List<String> num0List = new ArrayList<>();
    private List<String> num1List = new ArrayList<>();
    private List<String> num2List = new ArrayList<>();
    private List<Integer> num3List = new ArrayList<>();
    private List<Integer> num4List = new ArrayList<>();
    private List<String> num5List = new ArrayList<>();










    public List<String> outList = new ArrayList<>();
    public int numOfBase = 0;


    public int speCharPos = 0;


    private List<String> spe_cha_pos= new ArrayList<>();
    private List<String> spe_cha_ch= new ArrayList<>();
    private int spe_cha_len = 0;
    public void addSpe_cha_pos(int pos) {
        spe_cha_pos.add(String.valueOf(pos));
    }
    public void addSpe_cha_ch(int ch) {
        spe_cha_ch.add(String.valueOf(ch));
        spe_cha_len++;
    }


    public int n_letters_len = 0;
    public boolean n_flag = false;

    public List<String> nCha_begin = new ArrayList<>();
    private List<String> nCha_length = new ArrayList<>();
    public int nCha_len = 0;
    public void addnCha_begin(int begin) {
        nCha_begin.add(String.valueOf(begin));
    }
    public void addnCha_length(int length) {
        nCha_length.add(String.valueOf(length));
        nCha_len++;
    }


    public int preLineLen = 0;
    public int numSameLineLen = 0;

    private List<String> lineLens = new ArrayList<>();
    private List<String> numOfSameLineLens = new ArrayList<>();
    private int lineLensSize = 0;
    public void setNewLineLen(int newLineLen) {
        lineLens.add(String.valueOf(newLineLen));
        lineLensSize++;
    }
    public void setPreNumOfSameLineLen(int numOfSameLineLen) {
        numOfSameLineLens.add(String.valueOf(numOfSameLineLen));

    }


    public int prePosForFirstMatch = 0;
    public List<MatchEntry> me_t = new ArrayList<>();

    private static final int MAX_LINE_LEN = 1000;
    private Bases_Seq currentBaseFileBlock = new Bases_Seq();



    public static int PERCENT = -1;
    private static Map<Integer,List<MatchEntry>> matchList = new HashMap<>();
    private static Map<Integer,List<Integer>> seqLocVec = new HashMap<>();
    private static Map<Integer,int[]> seqBucketVec = new HashMap<>();




    public DealReads() {
        initial();
    }

    public void initial() {
    }


    public DealReads(Ref_base rb){
        this.rb = rb;
    }



    public void compressFirstAndSecondLinesForOneFastqFileBySpark(Iterator<String> lines) {

        int i = 0;
        while (lines.hasNext()) {
            linelen++;
            switch (i) {
                case 0:
                    firstLine(lines.next());
                    ++i;
                    break;
                case 1:
                    secondLine(lines.next());
                    ++i;
                    break;
                case 2:
                    lines.next();
                    ++i;
                    break;
                case 3:
                    lines.next();
                    i = 0;
                    break;
            }
        }


        saoWei();


        saveFlag(rb.getRefFile(), null, -1);



        saveOneFastQOtherInfo(outList);





        saveOneFastQIdentifierInfo(outList);








    }


    public void compressFirstAndSecondLinesForOneFastqFileBySpark2(Iterator<Tuple2<LongWritable, Text>> lines) {


        int i = 0;
        while (lines.hasNext()) {
            linelen++;
            switch (i) {
                case 0:
                    firstLine(lines.next()._2().toString());
                    ++i;
                    break;
                case 1:
                    secondLine(lines.next()._2().toString());
                    ++i;
                    break;
                case 2:
                    lines.next();
                    ++i;
                    break;
                case 3:
                    lines.next();
                    i = 0;
                    break;
            }
        }


        saoWei();


        saveFlag(rb.getRefFile(), null, -1);



        saveOneFastQOtherInfo(outList);




        saveOneFastQIdentifierInfo(outList);



    }




    public void compressFirstAndSecondLinesForOneFastqFile(File fp, int ii) {
        try {
            LineIterator iter = FileUtils.lineIterator(fp,"UTF-8");
            int i = 0;
            while (iter.hasNext()){
                linelen++;
                switch (i){
                    case 0:
                        firstLine(iter.nextLine());
                        ++i;
                        break;
                    case 1:
                        secondLine(iter.nextLine());
                        ++i;
                        break;
                    case 2:
                        iter.nextLine();
                        ++i;
                        break;
                    case 3:
                        iter.nextLine();
                        i=0;
                        break;
                }
            }
            saoWei();
            saveOneFastQOtherInfo(outList);
            saveOneFastQIdentifierInfo(outList);
            secondMatch(ii, outList);
            writeToFile(outList, bw[ii], rb.getRefFile(), fp, ii);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void saveMatchEntryForDiff(List<MatchEntry> list , String fileName) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
        bw.write(list.size());
        bw.newLine();
        for(int i = 0; i < list.size(); i++) {
            bw.write(list.get(i).getPos() + " " + list.get(i).getLength() + " " + list.get(i).getMisStr());
            bw.newLine();
        }
        bw.flush();
        bw.close();

        System.exit(0);

    }


    public void saveOneFastQIdentifierInfo(List<String> outList) {
        outList.add("*");
        outList.add(String.valueOf(nameList.size()));
        outList.addAll(nameList);
        outList.add("*");
        outList.add(String.valueOf(num0List.size()));
        outList.addAll(num0List);
        outList.add("*");
        outList.add(String.valueOf(name1List.size()));
        outList.addAll(name1List);
        outList.add("*");
        outList.add(String.valueOf(num1List.size()));
        outList.addAll(num1List);
        outList.add("*");
        outList.add(String.valueOf(num2List.size()));
        outList.addAll(num2List);
        outList.add("*");
        outList.add(String.valueOf(num3List.size()));
        for(int i = 0; i < num3List.size(); i++) {
            outList.add(String.valueOf(num3List.get(i)));
        }
        outList.add("*");
        outList.add(String.valueOf(num4List.size()));
        for(int i = 0; i < num4List.size(); i++) {
            outList.add(String.valueOf(num4List.get(i)));
        }
        outList.add("*");
        outList.add(String.valueOf(num5List.size()));
        outList.addAll(num5List);
        nameList = null;
        num2List = null;
        num3List = null;
        num4List = null;
        num5List = null;
    }



    public void writeToFile(List<String> outList, BufferedWriter bw, File ref, File tar, int ii){
        try {
            bw.write(">" + ref.getAbsolutePath() + " " /*+ tar.getAbsolutePath() + " " + ii + " "*/ + linelen + " " + numOfBase);
            bw.newLine();
            for (String s : outList) {
                bw.write(s);
                bw.newLine();
            }
            bw.newLine();

            bw.flush();
            outList.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void saveFlag(File ref, File tar, int ii) {
        outList.add(">" + ref.getAbsolutePath() + " " /*+ tar.getAbsolutePath() + " " + ii + " " */+ linelen + " " + numOfBase);
    }


    public void saoWei() {
        nameList.add(preName + " " + preNameNum);
        name1List.add(preName1 + " " + preNameNum1);
        num0List.add(baseNum0 + " " + preNum0ChaFenNum);
        num1List.add(preNum1 + " " + preNum1Num);
        num2List.add(preNum2 + " " + preNum2Num);
        num5List.add(preNum5 + " " + preNum5Num);
        if (n_flag) {
            addnCha_length(n_letters_len);
        } else {
            addnCha_begin(n_letters_len);
            addnCha_length(0);
        }
        assert nCha_begin.size() == nCha_length.size();
        setPreNumOfSameLineLen(numSameLineLen);
        assert lineLens.size() == numOfSameLineLens.size();
        if(currentBaseFileBlock.getSeq_len() > 0) {
            List<MatchEntry> temp = ComBase.codeFirstMatch(currentBaseFileBlock,rb, this);
            me_t.addAll(temp);
        }
    }


    public void saveOneFastQOtherInfo(List<String> outList) {
        outList.add("*");
        outList.add(String.valueOf(spe_cha_len));
        for(int k = 0; k < spe_cha_len; k++) {
            outList.add(spe_cha_pos.get(k) + " " + spe_cha_ch.get(k));
        }
        outList.add("*");
        outList.add(String.valueOf(nCha_len));
        for(int k = 0; k < nCha_len; k++) {
            outList.add(nCha_begin.get(k) + " " + nCha_length.get(k));
        }
        outList.add("*");
        outList.add(String.valueOf(lineLensSize));
        for(int k = 0; k < lineLensSize; k++) {
            outList.add(lineLens.get(k) + " " + numOfSameLineLens.get(k));
        }
        spe_cha_pos = null;
        spe_cha_ch = null;
        nCha_begin = null;
        nCha_length = null;
        lineLens = null;
        numOfSameLineLens = null;
    }


    public void readFinal(){
    }




    public void firstLine(String str){
        String[] tmp = str.split("[.:/]");
        String tname = tmp[0];
        String[] s = tmp[1].split(" ");
        int num0 = Integer.parseInt(s[0]);
        String tname1 = s[1];
        int num1 = Integer.parseInt(tmp[2]);
        int num2 = Integer.parseInt(tmp[3]);
        int num3 = Integer.parseInt(tmp[4]);
        int num4 = Integer.parseInt(tmp[5]);
        int num5 = Integer.parseInt(tmp[6]);
        if (preName!=null && !preName.equals("")) {
            if(preName.equals(tname)) {
                preNameNum++;
            } else {
                nameList.add(preName + " " + preNameNum);
                preName = tname;
                preNameNum = 1;
            }
            if(preName1.equals(tname1)) {
                preNameNum1++;
            } else {
                name1List.add(preName1 + " " + preNameNum1);
                preName1 = tname1;
                preNameNum1 = 1;
            }
            if(num0 - preNum0 == 1) {
                preNum0ChaFenNum++;
                preNum0 = num0;
            } else {
                num0List.add(baseNum0 + " " + preNum0ChaFenNum);
                baseNum0 = num0;
                preNum0 = num0;
                preNum0ChaFenNum = 0;
            }
            if(num1 == preNum1) {
                preNum1Num++;
            } else {
                num1List.add(preNum1 + " " + preNum1Num);
                preNum1 = num1;
                preNum1Num = 1;
            }
            if(num2 == preNum2) {
                preNum2Num++;
            } else {
                num2List.add(preNum2 + " " + preNum2Num);
                preNum2 = num2;
                preNum2Num = 1;
            }
            num3List.add(num3- preNum3);
            preNum3 = num3;
            num4List.add(num4- preNum4);
            preNum4 = num4;
            if(num5 == preNum5) {
                preNum5Num++;
            } else {
                num5List.add(preNum5 + " " + preNum5Num);
                preNum5 = num5;
                preNum5Num = 1;
            }
            return;
        }
        preName = tname;
        preName1 = tname1;
        baseNum0 = num0;
        preNum0 = num0;
        preNum1 = num1;
        preNum2 = num2;
        preNum3 = num3;
        preNum4 = num4;
        preNum5 = num5;
        preNameNum = 1;
        preNameNum1 = 1;
        preNum0ChaFenNum = 0;
        preNum1Num = 1;
        preNum2Num = 1;
        num3List.add(num3);
        num4List.add(num4);
        preNum5Num = 1;
    }


    public void secondLine(String str){
        numOfBase += str.length();
        if(currentBaseFileBlock.getSeq_len()+str.length() < 10100000-MAX_LINE_LEN){
            ComBase.seqLines(str, currentBaseFileBlock, this/*, n_letters_len, n_flag*/);
            return;
        }


        if(currentBaseFileBlock.getSeq_len()+str.length() < 10100000) {
            ComBase.seqLines(str, currentBaseFileBlock, this/*, n_letters_len, n_flag*/);
        } else {
            throw new ArrayIndexOutOfBoundsException(10100000);
        }


        List<MatchEntry> temp = ComBase.codeFirstMatch(currentBaseFileBlock, rb, this);
        me_t.addAll(temp);
        currentBaseFileBlock = new Bases_Seq();
    }


    private void secondMatch(int ii, List<String> outList){
        if(ii <= PERCENT){
            ComBase.matchResultHashConstruct(me_t, seqBucketVec, seqLocVec,ii);
            matchList.put(ii,me_t);
        }


        outList.add("*");
        if(ii==0) {
            for (MatchEntry matchEntry : me_t) {
                saveMatchEntry(matchEntry, outList);
            }
        }else {
            ComBase.codeSecondMatch(me_t, ii+1, seqBucketVec, seqLocVec, matchList, outList, PERCENT +1);
        }
        me_t = null;

    }

    public void saveMatchEntry(MatchEntry matchEntry, List<String> outList) {
        StringBuilder sbf = new StringBuilder();
        if(!matchEntry.getMisStr().isEmpty()){
            outList.add(matchEntry.getMisStr());
        }
        sbf.append(matchEntry.getPos()).append(' ').append(matchEntry.getLength());
        outList.add(sbf.toString());
    }

    private void runLengthCoding(char []vec , int length,File file) throws IOException{
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));

        List<Integer> code=new ArrayList<>();
        if (length > 0) {
            code.add((int) vec[0]);
            int cnt = 1;
            for (int i = 1; i < length; i++) {
                if (vec[i] - vec[i-1] == 0)
                    cnt++;
                else {
                    code.add(cnt);
                    code.add((int) vec[i]);
                    cnt = 1;
                }
            }
            code.add(cnt);

        }
        for (int c : code) {
            bw.write(c);
        }
        bw.newLine();
    }




}
