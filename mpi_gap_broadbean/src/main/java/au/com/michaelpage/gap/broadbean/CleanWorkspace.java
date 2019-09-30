package au.com.michaelpage.gap.broadbean;

import java.io.File;
import java.io.IOException;

import au.com.michaelpage.gap.common.util.DeleteDirUtil;

public class CleanWorkspace {

	public static void main(String[] args) throws IOException {
		DeleteDirUtil.delete(new File("C:\\GTS\\UK2\\hits\\"));
		//DeleteDirUtil.delete(new File("C:\\GTS\\hits\\"));
		DeleteDirUtil.delete(new File("GTSDB"));
		DeleteDirUtil.delete(new File("derby.log"));
	}
}
	
	
