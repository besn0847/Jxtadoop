package org.apache.jxtadoop.hdfs.p2p;

import java.util.Enumeration;
import net.jxta.document.Advertisement;
import net.jxta.document.AdvertisementFactory;
import net.jxta.document.Document;
import net.jxta.document.Element;
import net.jxta.document.ExtendableAdvertisement;
import net.jxta.document.MimeMediaType;
import net.jxta.document.StructuredDocument;
import net.jxta.document.StructuredDocumentFactory;
import net.jxta.document.TextElement;
import net.jxta.id.ID;

public class MulticastAdvertisement extends ExtendableAdvertisement {	
	public final static String AdvertisementType = "jxta:MulticastAdvertisement";
	
	private ID AdvertisementID = ID.nullID;
	private String LocalDnPeerID = "";
	private String RemoteDnPeerID = "";
	private final static String LocalDatanodeTag = "Local";
	private final static String RemoteDatanodeTag = "Remote";
	private final static String[] IndexableFields = { LocalDatanodeTag, RemoteDatanodeTag };
	
	public MulticastAdvertisement() {
	}
	
	@SuppressWarnings("rawtypes")
	public MulticastAdvertisement(Element root) {
		TextElement te = (TextElement) root;
        Enumeration elms = te.getChildren();
        
        while (elms.hasMoreElements()) {
           TextElement elm = (TextElement) elms.nextElement();
            ProcessElement(elm);
           
        }  
	}

	public MulticastAdvertisement(boolean formatted) {
		super(formatted);
	}

	@Override
	public String getBaseAdvType() {
		return AdvertisementType;
	}

	@Override
	public ID getID() {
		return AdvertisementID;
	}

	@Override
	public String[] getIndexFields() {
		return IndexableFields;
	}

	@Override
	public String getAdvType() {
		return AdvertisementType;
	}
	
	public static String getAdvertisementType() {
		return AdvertisementType;
	}
	
	public void setLocal(String id) {
		this.LocalDnPeerID = id;
	}

	public void setRemote(String id) {
		this.RemoteDnPeerID = id;
	}
	
	public String getLocal() {
		return this.LocalDnPeerID;
	}

	public String getRemote() {
		return this.RemoteDnPeerID;
	}
	
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Document getDocument(MimeMediaType mmt) {
		StructuredDocument result = StructuredDocumentFactory.newStructuredDocument(mmt,AdvertisementType);
		
		Element elm;
		
		elm = result.createElement(LocalDatanodeTag, LocalDnPeerID);
		result.appendChild(elm);
		
		elm = result.createElement(RemoteDatanodeTag,RemoteDnPeerID);
		result.appendChild(elm);
		
		return result;
	}
	
	@SuppressWarnings({ "rawtypes" })
	public void ProcessElement(TextElement te) {        
        String ten = te.getName();
        String ttv = te.getTextValue();
       
        if (ten.compareTo(LocalDatanodeTag)==0) {
        	LocalDnPeerID = ttv;
            return;
        }
       
        if (ten.compareTo(RemoteDatanodeTag)==0) {
        	RemoteDnPeerID = ttv;
            return;
        }
    }
	
	public static class Instantiator implements AdvertisementFactory.Instantiator {

		public String getAdvertisementType() {
			return MulticastAdvertisement.getAdvertisementType();
		}

		public Advertisement newInstance() {
			return new MulticastAdvertisement();
		}
		
		@SuppressWarnings({ "rawtypes" })
		public Advertisement newInstance(Element root) {
			return new MulticastAdvertisement(root);
		}	
	}
}
