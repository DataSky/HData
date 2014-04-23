package opensource.hdata.util;

import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

public class FTPUtils {

    public static FTPClient getFtpClient(String host, int port, String username, String password) throws SocketException, IOException {
        String LOCAL_CHARSET = "GB18030";
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(host, port);
        // 检测服务器是否支持UTF-8编码，如果支持就用UTF-8编码，否则就使用本地编码GB18030
        if (FTPReply.isPositiveCompletion(ftpClient.sendCommand("OPTS UTF8", "ON"))) {
            LOCAL_CHARSET = "UTF-8";
        }
        ftpClient.setControlEncoding(LOCAL_CHARSET);
        ftpClient.login(username, password);
        ftpClient.setBufferSize(1024 * 1024 * 16);
        ftpClient.enterLocalPassiveMode();
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        ftpClient.setControlKeepAliveTimeout(60);
        return ftpClient;
    }

    /**
     * 获取FTP目录下的文件
     * 
     * @param files
     * @param ftpClient
     * @param path
     *            FTP目录
     * @param filenameRegexp
     *            文件名正则表达式
     * @param recursive
     *            是否递归搜索
     * @throws IOException
     */
    public static void listFile(List<String> files, FTPClient ftpClient, String path, String filenameRegexp, boolean recursive) throws IOException {
        for (FTPFile ftpFile : ftpClient.listFiles(path)) {
            if (ftpFile.isFile()) {
                if (Pattern.matches(filenameRegexp, ftpFile.getName())) {
                    files.add(path + "/" + ftpFile.getName());
                }
            } else if (recursive && ftpFile.isDirectory()) {
                listFile(files, ftpClient, path + "/" + ftpFile.getName(), filenameRegexp, recursive);
            }
        }
    }

    /**
     * 关闭FTP客户端连接
     * 
     * @param ftpClient
     */
    public static void closeFtpClient(FTPClient ftpClient) {
        if (ftpClient != null) {
            try {
                ftpClient.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
