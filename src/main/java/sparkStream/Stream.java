package sparkStream;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.util.*;

import javax.swing.JFrame;
import javax.swing.JPanel;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

import org.opencv.core.Core;
//import org.apache.spark.streaming.kafka.KafkaUtils;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;

// implement from Apache Spark user docs

public class Stream extends JPanel


{
	
	
		/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
		static JFrame frame0 = new JFrame();
		 private BufferedImage image;


		 
		 public Stream()
		 {
			 
		 }
		public Stream(BufferedImage img) {
		        image = img;
		    }
		
		



		static {
			System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
		}
		
	public static String AdaBoostName = "D:\\8th sem\\BTP\\KafkaSparkSream\\haarcascade_frontalface_alt.xml";
		
		public static CascadeClassifier AdaBoost ;
	

	
	static int partitions;
	
	@Override
	 public void paint(Graphics g) {
	        g.drawImage(image, 0, 0, this);
	    }
	
	
	public static void main(String[] agrs ) throws InterruptedException {
		
	//	Mat frame2;
		
		AdaBoost = new CascadeClassifier();
		
		if (!AdaBoost.load(AdaBoostName))
		{
			System.out.print("Could not load AdaBoost model\n");

		}
		
Map<String, Object> kafkaParams = new HashMap<String, Object>();


kafkaParams.put("bootstrap.servers", "localhost:9092");
kafkaParams.put("max.partition.fetch.bytes", 2097152);
kafkaParams.put("key.deserializer", StringDeserializer.class);
kafkaParams.put("value.deserializer", StringDeserializer.class);
kafkaParams.put("group.id", "test");


Collection<String> topics = Arrays.asList("jbm");


SparkConf conf = new SparkConf().setAppName("KafkaInput");
// 1 sec batch size
JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(1));



final JavaInputDStream<ConsumerRecord<String, String>> stream =
  KafkaUtils.createDirectStream(
    jssc,
    LocationStrategies.PreferConsistent(),
    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
  );

JavaDStream<String> lines = stream.map(ConsumerRecord::value);

Scanner scan = new Scanner(System.in);
System.out.println("Enter the number of partitions required");
partitions=scan.nextInt();

scan.close();

lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void call(JavaRDD<String> rdd) { 
        JavaRDD<String> rowRDD = rdd.map(new Function<String, String>() {
            

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String msg) {
             
            	Encoder<SnapStream> encoder = Encoders.bean(SnapStream.class);
            	SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
            	
            	 Dataset<SnapStream> ds = spark.read().json(msg).as(encoder);
            	 
            	 ds.repartition(partitions);


         		ds.foreach(values -> {

         			SnapStream encoded;
         			Mat image ;
         			Mat frame2;
         			encoded = values;

         			image = new Mat(encoded.getRows(), encoded.getCols(), encoded.getType());
         			image.put(0, 0, Base64.getDecoder().decode(encoded.getData()));
/*
         		//	String imagePath = "/home/sai"+"WebCam-"+encoded.getTimestamp().getTime()+".png";

         			//Imgcodecs.imwrite(imagePath, image);
*/         			
         			frame2 =   detect(image);
         			
         			showUI(frame2);
         			
         		});
            	
            	
              return msg;
            }

			private void showUI(Mat frame2) {
				// TODO Auto-generated method stub
				
				BufferedImage img;
				
				img = bufferedImage(frame2);
				
				window(img,"Detected",100,50);
				
				
				
				
			}
			
			public void window(BufferedImage img, String text, int x, int y) {
        
        frame0.getContentPane().add(new Stream(img));
        frame0.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame0.setTitle(text);
        frame0.setSize(img.getWidth()+300, img.getHeight() + 300);
        frame0.setLocation(x, y);
        frame0.setVisible(true);
    }

			public BufferedImage bufferedImage(Mat m) {
			    int type = BufferedImage.TYPE_BYTE_GRAY;
			    if ( m.channels() > 1 ) {
			        type = BufferedImage.TYPE_3BYTE_BGR;
			    }
			    BufferedImage image = new BufferedImage(m.cols(),m.rows(), type);
			    m.get(0,0,((DataBufferByte)image.getRaster().getDataBuffer()).getData()); // get all the pixels
			    return image;
			}

			public Mat detect(Mat frame)
				{
					Mat frame_gray = new Mat();
					MatOfRect face = new MatOfRect();

				//	Rect[] facesArray = face.toArray();

					Imgproc.cvtColor(frame, frame_gray, Imgproc.COLOR_BGRA2GRAY);
					Imgproc.equalizeHist(frame_gray, frame_gray);


					AdaBoost.detectMultiScale( frame_gray, face, 1.1, 2, 0, new Size(30, 30), new Size() );
					
					Rect[] facesArray = face.toArray();
					
					System.out.println(facesArray.length);

					for (int i = 0; i < facesArray.length; i++)
					{

						Point center = new Point(facesArray[i].x + facesArray[i].width * 0.5, facesArray[i].y + facesArray[i].height * 0.5);
						Imgproc.ellipse(frame, center, new Size(facesArray[i].width * 0.5, facesArray[i].height * 0.5), 0, 0, 360, new Scalar(255, 0, 255), 4, 8, 0);

					//	Mat faceROI = frame_gray.submat(facesArray[i]);
					}

					return frame;

				}
			       
            
            
          });

       

    }
});


jssc.start();

jssc.awaitTermination();

}
}
class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
      if (instance == null) {
        instance = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate();
      }
      return instance;
    }
    
    
  
  }

