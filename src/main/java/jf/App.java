package jf;


import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.Job;

import java.util.UUID;


/**
 * Hello world!
 *
 */
public class App 
{

    private final static Logger logger_info = LoggerFactory.getLogger(App.class);
    private final static Logger logger_error = LoggerFactory.getLogger(App.class);

    public static void main( String[] args )
    {


        logger_info.debug("[================debug===============]");

        System.out.println("start!");


//        logger_info.info("s={}",args[0]);

        // Create a Parser
        CommandLineParser parser = new BasicParser();
        Options options = new Options();
        options.addOption("h", "help", false, "Print this usage information");
//        options.addOption("v", "verbose", false, "Print out VERBOSE information" );
        options.addOption("kjb", "kjb", true, "File to save program output to");
        options.addOption("ktr", "ktr", true, "File to save program output to");
//         Parse the program arguments
        String ktrFilePath = "";
        String kjbFilePath = "";
        try {
            CommandLine commandLine = parser.parse( options, args );
            ktrFilePath = commandLine.getOptionValue("ktr");
            kjbFilePath = commandLine.getOptionValue("kjb");

        } catch (ParseException e) {
            e.printStackTrace();
        }


        if (ktrFilePath!=null){
            System.out.println("[执行转换]-------"+ktrFilePath+"--------");
            App.runKettleTransfer(ktrFilePath);
            System.out.println("[执行转换完成]-----------------------------]");
        }

        if(kjbFilePath!=null){
            System.out.println("[执行作业]-------"+kjbFilePath+"--------");
            App.runJob(kjbFilePath);
            System.out.println("[执行作业完成]-----------------------------]");
        }



        System.out.println("end");
    }

    /**
     * 执行KETTLE转换
     * @param ktrFilePath
     * @return
     */
    public static boolean runKettleTransfer( String ktrFilePath) {

        Trans trans = null;

        String uuid = UUID.randomUUID().toString();

        logger_info.info("ExecKettleUtil@runKettleTransfer:"+uuid+" {ktrFilePath:"+ktrFilePath+"}");
        try {
            //初始化
            KettleEnvironment.init();
            EnvUtil.environmentInit();
            TransMeta transMeta = new TransMeta(ktrFilePath);
            //转换
            trans = new Trans(transMeta);

            //执行转换
            trans.execute(null);
            //等待转换执行结束
            trans.waitUntilFinished();
            if (trans.getErrors() > 0) {
                logger_info.info("ExecKettleUtil@runKettleTransfer:"+uuid+" 执行失败");
            }else{
                logger_info.info("ExecKettleUtil@runKettleTransfer:"+uuid+" 执行成功");
            }
            return true;
        } catch (Exception e) {
            logger_error.error("ExecKettleUtil@runKettleTransfer:"+uuid, e);
            return false;
        }
    }


    /**
     * 执行KETTLE作业
     * @param jobPath
     */
    public static void runJob(String jobPath) {
        try {
            KettleEnvironment.init();
            // jobname 是Job脚本的路径及名称
            JobMeta jobMeta = new JobMeta(jobPath, null);
            Job job = new Job(null, jobMeta);
            job.start();
            job.waitUntilFinished();
            if (job.getErrors() > 0) {
                throw new Exception(
                        "There are errors during job exception!(执行job发生异常)");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
