package es.upm.dit.dscc.DHT;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

import java.util.logging.ConsoleHandler;
//import java.util.logging.Filter;
//import java.util.logging.Handler;
import java.util.logging.Level;
//import java.util.logging.LogRecord;
import java.util.logging.Logger;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;



public class DHTMain extends JFrame{

	private JTextField textField;
    private JTextField textField_1;
    final JTextArea textArea;
    private static DHTManager dht;
    private static DHTMain mainDHT;
    
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format",
				"[%1$tF %1$tT][%4$-7s] [%5$s] [%2$-7s] %n");

		//    		"[%1$tF %1$tT] [%2$-7s] %3$s %n");
		//           "[%1$tF %1$tT] [%4$-7s] %5$s %n");
		//   "%4$s: %5$s [%1$tc]%n");
		//    "%1$tb %1$td, %1$tY %1$tl:%1$tM:%1$tS %1$Tp %2$s%n%4$s: %5$s%n");
	}

	static final Logger LOGGER = Logger.getLogger(DHTMain.class.getName());

	public DHTMain() {
		super("DHT Application - Zookeper Based");
	     
		//String   key    = null;
		//Integer value   = 0;
		

		//Para el servidor creado se genera el DHTManager
		dht = new DHTManager();
		
        // creates the GUI
        GridBagLayout gridBagLayout = new GridBagLayout();
        gridBagLayout.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0};
        gridBagLayout.columnWeights = new double[]{0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0};
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
        gbc_btnNewButton_1_1.insets = new Insets(0, 0, 5, 0);
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
        gbc_btnNewButton_2_1_1.insets = new Insets(0, 0, 5, 0);
        gbc_btnNewButton_2_1_1.gridx = 6;
        gbc_btnNewButton_2_1_1.gridy = 1;
        getContentPane().add(btnNewButton_2_1_1, gbc_btnNewButton_2_1_1);
        
        JButton btnNewButton_2_1_1_1 = new JButton("7) Exit");
        GridBagConstraints gbc_btnNewButton_2_1_1_1 = new GridBagConstraints();
        gbc_btnNewButton_2_1_1_1.insets = new Insets(0, 0, 5, 5);
        gbc_btnNewButton_2_1_1_1.gridx = 4;
        gbc_btnNewButton_2_1_1_1.gridy = 2;
        getContentPane().add(btnNewButton_2_1_1_1, gbc_btnNewButton_2_1_1_1);
        
        JLabel lblNewLabel = new JLabel("Introduzca la clave");
        GridBagConstraints gbc_lblNewLabel = new GridBagConstraints();
        gbc_lblNewLabel.anchor = GridBagConstraints.EAST;
        gbc_lblNewLabel.insets = new Insets(0, 0, 5, 5);
        gbc_lblNewLabel.gridx = 2;
        gbc_lblNewLabel.gridy = 3;
        getContentPane().add(lblNewLabel, gbc_lblNewLabel);
        
        textField = new JTextField();
        GridBagConstraints gbc_textField = new GridBagConstraints();
        gbc_textField.gridwidth = 2;
        gbc_textField.insets = new Insets(0, 0, 5, 5);
        gbc_textField.fill = GridBagConstraints.HORIZONTAL;
        gbc_textField.gridx = 4;
        gbc_textField.gridy = 3;
        getContentPane().add(textField, gbc_textField);
        textField.setColumns(10);
        
        JLabel lblIntroduzcaElValor = new JLabel("Introduzca el valor");
        GridBagConstraints gbc_lblIntroduzcaElValor = new GridBagConstraints();
        gbc_lblIntroduzcaElValor.anchor = GridBagConstraints.EAST;
        gbc_lblIntroduzcaElValor.insets = new Insets(0, 0, 5, 5);
        gbc_lblIntroduzcaElValor.gridx = 2;
        gbc_lblIntroduzcaElValor.gridy = 4;
        getContentPane().add(lblIntroduzcaElValor, gbc_lblIntroduzcaElValor);
        
        textField_1 = new JTextField();
        textField_1.setColumns(10);
        GridBagConstraints gbc_textField_1 = new GridBagConstraints();
        gbc_textField_1.gridwidth = 2;
        gbc_textField_1.insets = new Insets(0, 0, 5, 5);
        gbc_textField_1.fill = GridBagConstraints.HORIZONTAL;
        gbc_textField_1.gridx = 4;
        gbc_textField_1.gridy = 4;
        getContentPane().add(textField_1, gbc_textField_1);
        
        textArea = new JTextArea();
        GridBagConstraints gbc_textArea = new GridBagConstraints();
        gbc_textArea.insets = new Insets(0, 0, 5, 0);
        gbc_textArea.gridwidth = 3;
        gbc_textArea.fill = GridBagConstraints.BOTH;
        gbc_textArea.gridx = 5;
        gbc_textArea.gridy = 6;
        getContentPane().add(textArea, gbc_textArea);
        
        JLabel lblResultado = new JLabel("Resultado");
        GridBagConstraints gbc_lblResultado = new GridBagConstraints();
        gbc_lblResultado.insets = new Insets(0, 0, 0, 5);
        gbc_lblResultado.gridx = 4;
        gbc_lblResultado.gridy = 6;
        getContentPane().add(lblResultado, gbc_lblResultado);
       

        
        // adds event handler for button Put
        btnNewButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent evt) {
            	String key= textField.getText().toString();
            	int value = Integer.parseInt(textField_1.getText().toString());
            	DHT_Map map = new DHT_Map (key,value);
            	if (!dht.isQuorum()) {
            		textArea.setText("No hay quorum. No es posible ejecutar su elección");
					
				}
            	else{
            	int resultado = dht.put(map);
            	textArea.setText(Integer.toString(resultado));
            	}
            }
        });
        
        
        // adds event handler for button Get
        btnNewButton_1.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent evt) {
            	if (!dht.isQuorum()) {
            		textArea.setText("No hay quorum. No es posible ejecutar su elección");
					
				}
            	else {
            	int value  = dht.get(textField.getText().toString());
				
	            	textArea.setText(Integer.toString(value));
            	}
            }
        });
         
     // adds event handler for button Remove
        btnNewButton_1_1.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent evt) {
            	if (!dht.isQuorum()) {
            		textArea.setText("No hay quorum. No es posible ejecutar su elección");
					
				}
            	else {
            	int value  = dht.remove(textField.getText().toString());
				
	            	textArea.setText(Integer.toString(value));
            	}
            }
        });
        
    
        // adds event handler for button Containkey
        btnNewButton_2.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent evt) {
            	if (!dht.isQuorum()) {
            		textArea.setText("No hay quorum. No es posible ejecutar su elección");
					
				}
            	else {
            	if (dht.containsKey(textField.getText().toString())) {
            		textArea.setText("This key is contained");						
				} else {
					textArea.setText("The option is not contained");	
				}   	
            	}
            }
        });
        
    // adds event handler for button Values
        btnNewButton_2_1.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(ActionEvent evt) {
        	if (!dht.isQuorum()) {
        		textArea.setText("No hay quorum. No es posible ejecutar su elección");
				
			}else {
        	textArea.setText("List of values in the DHT: \n"+ dht.toString());
        	}
        }
        });
     
     // adds event handler for button Init
        btnNewButton_2_1_1.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(ActionEvent evt) {
        	if (!dht.isQuorum()) {
        		textArea.setText("No hay quorum. No es posible ejecutar su elección");
				
			}else {
				initMembers(dht);
        	}
        }
        });
        
        // adds event handler for button Exit
        btnNewButton_2_1_1_1.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(ActionEvent evt) {
        	System.exit(0);
        	
        }
        });
        
        
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    setSize(480, 320);
    setLocationRelativeTo(null);    // centers on screen
		configureLogger();
	}



	
	public void configureLogger() {
		ConsoleHandler handler;
		handler = new ConsoleHandler(); 
		handler.setLevel(Level.FINEST); 
		LOGGER.addHandler(handler); 
		LOGGER.setLevel(Level.FINEST);
	}

	//////////////////////////////////////////////////////////////////////////

	public void initMembers(DHTUserInterface dht) {

		//if (!dht.containsKey("Angel")) {
			dht.put(new DHT_Map("Angel", 1));
		//}
		//if (!dht.containsKey("Bernardo")) {
			dht.put(new DHT_Map("Bernardo", 2));
		//}
		//if (!dht.containsKey("Carlos")) {
			dht.put(new DHT_Map("Carlos", 3));
		//}
		//if (!dht.containsKey("Daniel")) {
			dht.put(new DHT_Map("Daniel", 4));
		//}
		//if (!dht.containsKey("Eugenio")) {
			dht.put(new DHT_Map("Eugenio", 5));
		//}
		//if (!dht.containsKey("Zamorano")) {
			dht.put(new DHT_Map("Zamorano", 6));
		//}
	}

	//////////////////////////////////////////////////////////////////////////

	public DHT_Map putMap(Scanner sc) {
		String  key     = null;
		Integer value   = 0;

		System. out .print(">>> Enter name (String) = ");
		key = sc.next();


		System. out .print(">>> Enter account number (int) = ");
		if (sc.hasNextInt()) {
			value = sc.nextInt();
		} else {
			System.out.println("The provised text provided is not an integer");
			sc.next();
			return null;
		}

		return new DHT_Map(key, value);
	}

	//////////////////////////////////////////////////////////////////////////
	//TODO >> La mejora de la interfaz se hace modificando este main
	public static void main(String[] args) {
		System.out.println("Version basada en ZooKeeper");
		System.out.println("Welcome to ZKCluster | DVN-RTG-SFB | Grupo 1");
		try {
			Thread.sleep(5000); //Timer para la correcta configuracion de zookeeper entre ejecuciones consecutivas
		}catch(Exception e) {
			
		}
		mainDHT = new DHTMain();
		SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                mainDHT.setVisible(true);
            }
        });
		
		
		System.out.println("Version basada en Zookeeper");

		boolean correct = false;
		int     menuKey = 0;
		boolean exit    = false;
		Scanner sc      = new Scanner(System.in);


		String   key    = null;
		Integer value   = 0;
		
		//Configura DHTMain (interfaz) y Logger 
		

		//Para el servidor creado se genera el DHTManager
		

		while (!exit) {
			try {
				correct = false;
				menuKey = 0;
				while (!correct) {
					System. out .println(">>> Enter option: 1) Put. 2) Get. 3) Remove. 4) ContainKey  5) Values 7) Init 0) Exit");				
					if (sc.hasNextInt()) {
						menuKey = sc.nextInt();
						correct = true;
					} else {
						sc.next();
						System.out.println("The provised text provided is not an integer");
					}
					
				}
				if (!dht.isQuorum()) {
					System.out.println("No hay quorum. No es posible ejecutar su elección");
					continue;
				}
				
				switch (menuKey) {
				case 1: // Put
					dht.put(mainDHT.putMap(sc));
					break;

				case 2: // Get
					System. out .print(">>> Enter key (String) = ");
					key    = sc.next();
					value  = dht.get(key);
					if (value != null) {
						System.out.println(value);							
					} else {
						System.out.println("The key: " + key + " does not exist");
					}

					break;
				case 3: // Remove
					System. out .print(">>> Enter key (String) = ");
					key    = sc.next();
					//if (dht.containsKey(key)) {
					value  = dht.remove(key);
					if (value != null) {
						System.out.println(value);							
					} else {
						System.out.println("The key: " + key + " does not exist");
					}					
					break;
				case 4: // ContainKey
					System. out .print(">>> Enter key (String) = ");
					key    = sc.next();
					if (dht.containsKey(key)) {
						System.out.println("This key is contained");						
					} else {
						System.out.println("The option is not contained");						
					}
					break;
				case 5:
					//ArrayList<Integer> list = new ArrayList<Integer>();
					System.out.println("List of values in the DHT:");
					System.out.println(dht.toString());
					break;
				case 6:
					//Set<String> set = new HashSet<String>();
					//set = dht.keySet();
					//for (Iterator iterator = set.iterator(); iterator.hasNext();) {
					//	String string = (String) iterator.next();
					//	Integer valueSet = dht.get(string);
					//	System.out.print("["+ string + ", "  + valueSet+ "] ");
					//}
					System.out.println("The option is not available");
					break;

				case 7:
					mainDHT.initMembers(dht);
					break;

				case 0:
					exit = true;	
					//dht.close();
				default:
					break;
				}
			} catch (Exception e) {
				System.out.println("Exception at Main. Error read data");
				System.err.println(e);
				e.printStackTrace();
			}

		}

		sc.close();
	}
}