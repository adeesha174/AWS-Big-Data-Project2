package Jobs_NoCombiners;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import software.amazon.awssdk.services.ec2.model.InstanceType;

public class MainFlow {
    public static void main(String[] args) {
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(credentialsProvider)
                .build();

        // ---------------------------------- Step1 ---------------------------------- //
        // ---------------------------------- Calculating r0 and r1 ---------------------------------- //
        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar("s3n://jar-bucket-ass2/JFS_calc_r.jar")
                .withMainClass("Jobs_NoCombiners.JFS_calc_r")
                .withArgs("s3n://jar-bucket-ass2/corpus0/*",
                        "s3n://jar-bucket-ass2/corpus1/*",
                        "s3n://output-bucket-ass2/JFS_calc_r0_r1_output/");

        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step2 ---------------------------------- //
        // ---------------------------------- Calculating Tr01 ---------------------------------- //
        // ---------------------------------- Calculating Tr10 ---------------------------------- //
        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://jar-bucket-ass2/JFS_calc_Tr.jar")
                .withMainClass("Jobs_NoCombiners.JFS_calc_Tr")
                .withArgs("s3n://output-bucket-ass2/JFS_calc_r0_r1_output/*",
                        "s3n://output-bucket-ass2/JFS_calc_Tr01_output/",
                        "s3n://output-bucket-ass2/JFS_calc_Tr10_output/");

        StepConfig stepConfig2 = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step3 ---------------------------------- //
        // ---------------------------------- Calculating Nr0 ---------------------------------- //
        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://jar-bucket-ass2/JFS_calc_Nr.jar")
                .withMainClass("Jobs_NoCombiners.JFS_calc_Nr")
                .withArgs("0",
                        "s3n://output-bucket-ass2/JFS_calc_r0_r1_output/*",
                        "s3n://output-bucket-ass2/JFS_calc_Nr0_intermediate/",
                        "s3n://output-bucket-ass2/JFS_calc_Nr0_output/");

        StepConfig stepConfig3 = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step4 ---------------------------------- //
        // ---------------------------------- Calculating Nr1 ---------------------------------- //
        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar("s3n://jar-bucket-ass2/JFS_calc_Nr.jar")
                .withMainClass("Jobs_NoCombiners.JFS_calc_Nr")
                .withArgs("1",
                        "s3n://output-bucket-ass2/JFS_calc_r0_r1_output/*",
                        "s3n://output-bucket-ass2/JFS_calc_Nr1_intermediate/",
                        "s3n://output-bucket-ass2/JFS_calc_Nr1_output/");

        StepConfig stepConfig4 = new StepConfig()
                .withName("Step4")
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step5 ---------------------------------- //
        // ---------------------------------- Calculating Tr01 + tr10 ---------------------------------- //
        // ---------------------------------- Calculating Nr0 + Nr1 ---------------------------------- //
        HadoopJarStepConfig hadoopJarStep5 = new HadoopJarStepConfig()
                .withJar("s3n://jar-bucket-ass2/JFS_sum.jar")
                .withMainClass("Jobs_NoCombiners.JFS_sum")
                .withArgs("s3n://output-bucket-ass2/JFS_calc_Tr01_output/*",
                        "s3n://output-bucket-ass2/JFS_calc_Tr10_output/*",
                        "s3n://output-bucket-ass2/JFS_sum_Tr01_Tr10_output/",
                        "s3n://output-bucket-ass2/JFS_calc_Nr0_output/*",
                        "s3n://output-bucket-ass2/JFS_calc_Nr1_output/*",
                        "s3n://output-bucket-ass2/JFS_sum_Nr0_Nr1_output/");

        StepConfig stepConfig5 = new StepConfig()
                .withName("Step5")
                .withHadoopJarStep(hadoopJarStep5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step6 ---------------------------------- //
        // ---------------------------------- Calculating N(Nr0 + Nr1) ---------------------------------- //
        HadoopJarStepConfig hadoopJarStep6 = new HadoopJarStepConfig()
                .withJar("s3n://jar-bucket-ass2/JFS_calc_N.jar")
                .withMainClass("Jobs_NoCombiners.JFS_calc_N")
                .withArgs("s3n://output-bucket-ass2/JFS_calc_r0_r1_output/*",
                        "s3n://output-bucket-ass2/JFS_calc_N_output/",
                        "s3n://output-bucket-ass2/JFS_sum_Nr0_Nr1_output/*",
                        "s3n://output-bucket-ass2/JFS_multiply_output/");

        StepConfig stepConfig6 = new StepConfig()
                .withName("Step6")
                .withHadoopJarStep(hadoopJarStep6)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step7 ---------------------------------- //
        // ---------------------------------- Calculating division ---------------------------------- //
        HadoopJarStepConfig hadoopJarStep7 = new HadoopJarStepConfig()
                .withJar("s3n://jar-bucket-ass2/JFS_divide.jar")
                .withMainClass("Jobs_NoCombiners.JFS_divide")
                .withArgs("s3n://output-bucket-ass2/JFS_sum_Tr01_Tr10_output/*",
                        "s3n://output-bucket-ass2/JFS_multiply_output/*",
                        "s3n://output-bucket-ass2/JFS_divide_output/");

        StepConfig stepConfig7 = new StepConfig()
                .withName("Step7")
                .withHadoopJarStep(hadoopJarStep7)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- Step8 ---------------------------------- //
        // ---------------------------------- Calculating result ---------------------------------- //
        HadoopJarStepConfig hadoopJarStep8 = new HadoopJarStepConfig()
                .withJar("s3n://jar-bucket-ass2/JFS_final_result.jar")
                .withMainClass("Jobs_NoCombiners.JFS_final_result")
                .withArgs("s3n://output-bucket-ass2/JFS_divide_output/*",
                        "s3n://output-bucket-ass2/JFS_final_result_output/");

        StepConfig stepConfig8 = new StepConfig()
                .withName("Step8")
                .withHadoopJarStep(hadoopJarStep8)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // ---------------------------------- RunJobFlow ---------------------------------- //
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M4_LARGE.toString())
                .withSlaveInstanceType(InstanceType.M4_LARGE.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("ass2KeyPair")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Ass2AdeeOfek")
                .withReleaseLabel("emr-5.14.0")
                .withInstances(instances)
                .withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4,
                        stepConfig5, stepConfig6, stepConfig7, stepConfig8)
                .withLogUri("s3n://ass2-log-bucket/")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
