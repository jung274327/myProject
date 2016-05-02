import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;   

import com.ahnlab.sts.common.Util;      



public class SampleFileSizeCheck       {
	
	private Connection con;
	private PreparedStatement pstmt;
	private ResultSet rs;
	private long notSampleFileSize;
	private String base_path = "/test/test/test/extract/";
	
	private void createConnection() throws Exception  {
		try {
			String driver = "oracle.jdbc.driver.OracleDriver";
			String urlSvc = "jdbc:oracle:thin:@172.16.80.89:3559:hao";
			String dbuser = "blueberry";
			String dbpwd = "blueberry";

			Class.forName(driver);
			con = DriverManager.getConnection(urlSvc, dbuser, dbpwd);
			if ( con == null ) {
				throw new SQLException("Connection is null!");
			}

			con.setAutoCommit(true);

		} catch ( Exception e ) {
			System.out.println(" [X] .main : exception=" + e.toString());
			throw e;
		}
	}
	
	private long getFileList(String samplepath, String year, String month, String day) {
		File isFile = new File(Util.concatePath(samplepath, getFileDayPath(year, month, day)));
		if ( !isFile.exists() ) return 0;
		
		File[] fileList = isFile.getAbsoluteFile().listFiles();
		long filesize = 0;
		for ( File file : fileList ) {
			if ( ".DS_Store".equals(file.getName()) ) continue;
			filesize += getFileSize(file);
		}
		return filesize;
	}
	
	private long getFileSize(File orgFile) {
		File[] fileList = orgFile.listFiles();
		long filesize = 0;
		for ( File file : fileList ) {
			if ( file.isFile() ) {
				String sample_code = (file.getParentFile().getAbsolutePath().replaceAll("/Volumes/smpbak/sample/extract/", "")).replace("/", "") + 
						"-" + file.getName();
				if ( !isSampleCode(sample_code) ) {
					notSampleFileSize += file.length();
					System.out.println(sample_code);
				} else filesize += file.length();
			}
		}
		return filesize;
	}
	
	
	private String getFileDayPath(String year, String month, String day) {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, Integer.parseInt(year));
		cal.set(Calendar.MONTH, Integer.parseInt(month)-1);
		cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
		
		//System.out.println(new SimpleDateFormat("yy/MM/dd").format(cal.getTime()));
		
		return new SimpleDateFormat("yy/MM/dd").format(cal.getTime());
		
	}
	
	private boolean isSampleCode(String sample_code) {
		String query = "select sample_code from sp_sample_info where sample_code = ? ";
		
		try {
			createConnection();
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, sample_code);
			rs = pstmt.executeQuery();
			return rs.next();
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if ( rs != null ) rs.close();
				if ( pstmt != null ) pstmt.close();
				if ( con != null ) con.close();
			} catch(Exception ex) {}
		}
		return false;
	}
	
	
	public static void main(String[] args) throws Exception {
		SampleFileSizeCheck perm = new SampleFileSizeCheck();
		
		File[] fileList = new File(perm.base_path).listFiles();
		long filesize = 0;
		
		for ( File file : fileList ) {
			if ( file.isFile() ) continue;
			System.out.println("main : "+file.getAbsolutePath());
			filesize += perm.getFileList(file.getAbsolutePath(), "2010", "05", "01");
		}
		System.out.println("filesize : "+NumberFormat.getInstance().format(filesize));
		System.out.println("not a SampleCode is File Size : "+NumberFormat.getInstance().format(perm.notSampleFileSize));
	}
	
}
