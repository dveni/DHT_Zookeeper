package prueba;


import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.PrintStream;

 
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

 
public class demo2 extends JFrame {
     
    
    private PrintStream standardOut;
     
    public demo2() {
        super("DHT Application - Zookeper Based");
     
        
    
        // creates the GUI
        GridBagLayout gridBagLayout = new GridBagLayout();
        gridBagLayout.rowWeights = new double[]{0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0};
        gridBagLayout.columnWeights = new double[]{0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        getContentPane().setLayout(gridBagLayout);
        
        JButton btnNewButton = new JButton("1) Put");
        GridBagConstraints gbc_btnNewButton = new GridBagConstraints();
        gbc_btnNewButton.insets = new Insets(0, 0, 5, 5);
        gbc_btnNewButton.gridx = 2;
        gbc_btnNewButton.gridy = 0;
        getContentPane().add(btnNewButton, gbc_btnNewButton);
        
        JButton btnNewButton_1 = new JButton("2) Get");
        GridBagConstraints gbc_btnNewButton_1 = new GridBagConstraints();
        gbc_btnNewButton_1.insets = new Insets(0, 0, 5, 5);
        gbc_btnNewButton_1.gridx = 4;
        gbc_btnNewButton_1.gridy = 0;
        getContentPane().add(btnNewButton_1, gbc_btnNewButton_1);
        
        JButton btnNewButton_1_1 = new JButton("3) Remove");
        GridBagConstraints gbc_btnNewButton_1_1 = new GridBagConstraints();
        gbc_btnNewButton_1_1.insets = new Insets(0, 0, 5, 5);
        gbc_btnNewButton_1_1.gridx = 6;
        gbc_btnNewButton_1_1.gridy = 0;
        getContentPane().add(btnNewButton_1_1, gbc_btnNewButton_1_1);
        
        JButton btnNewButton_2 = new JButton("4) ContainKey");
        GridBagConstraints gbc_btnNewButton_2 = new GridBagConstraints();
        gbc_btnNewButton_2.insets = new Insets(0, 0, 5, 5);
        gbc_btnNewButton_2.gridx = 2;
        gbc_btnNewButton_2.gridy = 1;
        getContentPane().add(btnNewButton_2, gbc_btnNewButton_2);
        
        JButton btnNewButton_2_1 = new JButton("5) Values");
        GridBagConstraints gbc_btnNewButton_2_1 = new GridBagConstraints();
        gbc_btnNewButton_2_1.insets = new Insets(0, 0, 5, 5);
        gbc_btnNewButton_2_1.gridx = 4;
        gbc_btnNewButton_2_1.gridy = 1;
        getContentPane().add(btnNewButton_2_1, gbc_btnNewButton_2_1);
        
        JButton btnNewButton_2_1_1 = new JButton("6) Init");
        GridBagConstraints gbc_btnNewButton_2_1_1 = new GridBagConstraints();
        gbc_btnNewButton_2_1_1.insets = new Insets(0, 0, 5, 5);
        gbc_btnNewButton_2_1_1.gridx = 6;
        gbc_btnNewButton_2_1_1.gridy = 1;
        getContentPane().add(btnNewButton_2_1_1, gbc_btnNewButton_2_1_1);
        
        JButton btnNewButton_2_1_1_1 = new JButton("7) Exit");
        GridBagConstraints gbc_btnNewButton_2_1_1_1 = new GridBagConstraints();
        gbc_btnNewButton_2_1_1_1.insets = new Insets(0, 0, 5, 5);
        gbc_btnNewButton_2_1_1_1.gridx = 4;
        gbc_btnNewButton_2_1_1_1.gridy = 2;
        getContentPane().add(btnNewButton_2_1_1_1, gbc_btnNewButton_2_1_1_1);

        JTextArea textArea_1 = new JTextArea();
        GridBagConstraints gbc_textArea_1 = new GridBagConstraints();
        gbc_textArea_1.gridwidth = 8;
        gbc_textArea_1.gridheight = 4;
        gbc_textArea_1.insets = new Insets(0, 0, 5, 0);
        gbc_textArea_1.fill = GridBagConstraints.BOTH;
        gbc_textArea_1.gridx = 0;
        gbc_textArea_1.gridy = 3;
        getContentPane().add(textArea_1, gbc_textArea_1);
        
        textArea_1.setEditable(true);
        PrintStream printStream = new PrintStream(new CustomOutputStream(textArea_1));
       
        
        // keeps reference of standard output stream
        standardOut = System.out;

        
        // re-assigns standard output stream and error output stream
        System.setOut(printStream);
        System.setErr(printStream);
        
        // adds event handler for button Put
        btnNewButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent evt) {
            	
            	//dht.put(mainDHT.putMap(sc));
            	
            }
        });
        
        
        // adds event handler for button Get
        btnNewButton_1.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent evt) {
                printLogGet();
            }
        });
         
         
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(480, 320);
        setLocationRelativeTo(null);    // centers on screen
    }
     
    /**
     * Prints log statements for testing in a thread
     */
    private void printLogGet() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                
                	System. out .print(">>> Enter key (String) = ");
//					key    = sc.next();
//					value  = dht.get(key);
//					if (value != null) {
//						System.out.println(value);							
//					} else {
//						System.out.println("The key: " + key + " does not exist");
//					}

                    
            }
        });
        thread.start();
    }
     
    /**
     * Runs the program
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                new demo2().setVisible(true);
            }
        });
    }
}
