package org.radargun.microbenchmark.domain;

import java.io.Serializable;
import java.util.UUID;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;

public class IntSetRBTree implements IntSet{

    public enum Color {BLACK, RED};

    private int myNode;
    private RBNode mySentinelNode;
    
    private void setRoot(CacheWrapper wrapper, RBNode value) {
        getRoot(wrapper).setLeft(wrapper, value);
    }
    
    private RBNode getRoot(CacheWrapper wrapper) {
        LocatedKey key = wrapper.createKey(myNode + ":root", myNode);
        return ((RBNode) Micro.get(wrapper, key)).getLeft(wrapper);
    }

    public IntSetRBTree(int myNode, CacheWrapper cache) {
        this.myNode = myNode;
        
        mySentinelNode = new RBNode(true);
        mySentinelNode.setValue(cache, 123456);
        mySentinelNode.setLeft(cache, null);
        mySentinelNode.setRight(cache, null);
        mySentinelNode.setParent(cache, null);
        mySentinelNode.setColor(cache, Color.BLACK);
        
        RBNode aux = new RBNode();
        aux.setLeft(cache, mySentinelNode);
        aux.setValue(cache, Integer.MIN_VALUE);
        aux.setColor(cache, Color.BLACK);
        LocatedKey key = cache.createKey(myNode + ":root", myNode);
        Micro.put(cache, key, aux);
    }

    public boolean add(CacheWrapper cache, final int key) {
        RBNode node	= new RBNode();
        RBNode temp	= getRoot(cache);

        while(!temp.isSentinel()) {	// find Parent
            node.setParent(cache, temp);
            if ( key == temp.getValue(cache)) {
                return false;
            } else if (key > temp.getValue(cache)) {
                temp = temp.getRight(cache);
            } else {
                temp = temp.getLeft(cache);
            }
        }

        // setup node
        node.setValue(cache, key);
        node.setLeft(cache, mySentinelNode);
        node.setRight(cache, mySentinelNode);

        // insert node into tree starting at parent's location
        if(node.getParent(cache) != null) {
            if (node.getValue(cache) > node.getParent(cache).getValue(cache)) {
                node.getParent(cache).setRight(cache, node);
            } else
                node.getParent(cache).setLeft(cache, node);
        } else
            setRoot(cache, node); 		// first node added

            restoreAfterInsert(cache, node);           // restore red-black properities
            return true;
    }

    public boolean contains(CacheWrapper cache, final int key) {
        RBNode node = getRoot(cache);     // begin at root

        // traverse tree until node is found
        while(!node.isSentinel()) {
            if (key == node.getValue(cache)) {
                return true;
            } else if (key < node.getValue(cache)) {
                node = node.getLeft(cache);
            } else {
                node = node.getRight(cache);
            }
        }
        return false;
    }

    public boolean remove(CacheWrapper cache, final int key) {
        RBNode node;

        node = getRoot(cache);
        while(!node.isSentinel()) {
            if (key == node.getValue(cache)) {
                break;
            } else if (key < node.getValue(cache)) {
                node = node.getLeft(cache);
            } else {
                node = node.getRight(cache);
            }
        }

        if(!node.isSentinel())
            return false;				// key not found

            delete(cache, node);
            return true;
    }

    private void delete(CacheWrapper cache, RBNode z) {
        RBNode x = new RBNode();
        RBNode y;
        
        if(z.getLeft(cache).isSentinel() || z.getRight(cache).isSentinel())
            y = z;
        else {
            y = z.getRight(cache);
            while(!y.getLeft(cache).isSentinel())
                y = y.getLeft(cache);
        }

        if(!y.getLeft(cache).isSentinel())
            x = y.getLeft(cache);
        else
            x = y.getRight(cache);

        x.setParent(cache, y.getParent(cache));
        if(y.getParent(cache) != null)
            if(y == y.getParent(cache).getLeft(cache))
                y.getParent(cache).setLeft(cache, x);
            else
                y.getParent(cache).setRight(cache, x);
        else
            setRoot(cache, x);

        if(y != z) {
            z.setValue(cache, y.getValue(cache));
        }

        if(y.getColor(cache) == Color.BLACK)
            restoreAfterDelete(cache, x);
    }

    private void restoreAfterDelete(CacheWrapper cache, RBNode x) {
        RBNode y;

        while(x != getRoot(cache) && x.getColor(cache) == Color.BLACK) {
            if(x == x.getParent(cache).getLeft(cache))			// determine sub tree from parent
            {
                y = x.getParent(cache).getRight(cache);			// y is x's sibling
                if(y.getColor(cache) == Color.RED) {	// x is black, y is red - make both black and rotate
                    y.setColor(cache, Color.BLACK);
                    x.getParent(cache).setColor(cache, Color.RED);
                    rotateLeft(cache, x.getParent(cache));
                    y = x.getParent(cache).getRight(cache);
                }
                if(y.getLeft(cache).getColor(cache) == Color.BLACK &&
                        y.getRight(cache).getColor(cache) == Color.BLACK) {	// children are both black
                    y.setColor(cache, Color.RED); // change parent to red
                    x = x.getParent(cache); // move up the tree
                } else {
                    if(y.getRight(cache).getColor(cache) == Color.BLACK) {
                        y.getLeft(cache).setColor(cache, Color.BLACK);
                        y.setColor(cache, Color.RED);
                        rotateRight(cache, y);
                        y				= x.getParent(cache).getRight(cache);
                    }
                    y.setColor(cache, x.getParent(cache).getColor(cache));
                    x.getParent(cache).setColor(cache, Color.BLACK);
                    y.getRight(cache).setColor(cache, Color.BLACK);
                    rotateLeft(cache, x.getParent(cache));
                    setRoot(cache, x);
                }
            } else {	// right subtree - same as code above with right and left swapped
                y = x.getParent(cache).getLeft(cache);
                if(y.getColor(cache) == Color.RED) {
                    y.setColor(cache, Color.BLACK);
                    x.getParent(cache).setColor(cache, Color.RED);
                    rotateRight(cache, x.getParent(cache));
                    y = x.getParent(cache).getLeft(cache);
                }
                if(y.getRight(cache).getColor(cache) == Color.BLACK &&
                        y.getLeft(cache).getColor(cache) == Color.BLACK) {
                    y.setColor(cache, Color.RED);
                    x		= x.getParent(cache);
                } else {
                    if(y.getLeft(cache).getColor(cache) == Color.BLACK) {
                        y.getRight(cache).setColor(cache, Color.BLACK);
                        y.setColor(cache, Color.RED);
                        rotateLeft(cache, y);
                        y				= x.getParent(cache).getLeft(cache);
                    }
                    y.setColor(cache, x.getParent(cache).getColor(cache));
                    x.getParent(cache).setColor(cache, Color.BLACK);
                    y.getLeft(cache).setColor(cache, Color.BLACK);
                    rotateRight(cache, x.getParent(cache));
                    setRoot(cache, x);
                }
            }
        }
        x.setColor(cache, Color.BLACK);
    }

    private void restoreAfterInsert(CacheWrapper cache, RBNode x) {
        RBNode y;

        while(x != getRoot(cache) && x.getParent(cache).getColor(cache) == Color.RED) {
            if(x.getParent(cache) == x.getParent(cache).getParent(cache).getLeft(cache))	// determine traversal path
            {										// is it on the Left or Right subtree?
                y = x.getParent(cache).getParent(cache).getRight(cache);			// get uncle
                if(y!= null && y.getColor(cache) == Color.RED) {	// uncle is red; change x's Parent and uncle to black
                    x.getParent(cache).setColor(cache, Color.BLACK);
                    y.setColor(cache, Color.BLACK);
                    // grandparent must be red. Why? Every red node that is not
                    // a leaf has only black children
                    x.getParent(cache).getParent(cache).setColor(cache, Color.RED);
                    x						= x.getParent(cache).getParent(cache);	// continue loop with grandparent
                } else {
                    // uncle is black; determine if x is greater than Parent
                    if(x == x.getParent(cache).getRight(cache)) {	// yes, x is greater than Parent; rotate Left
                        // make x a Left child
                        x = x.getParent(cache);
                        rotateLeft(cache, x);
                    }
                    // no, x is less than Parent
                    x.getParent(cache).setColor(cache, Color.BLACK);	// make Parent black
                    x.getParent(cache).getParent(cache).setColor(cache, Color.RED);		// make grandparent black
                    rotateRight(cache, x.getParent(cache).getParent(cache));					// rotate right
                }
            } else {	// x's Parent is on the Right subtree
                // this code is the same as above with "Left" and "Right" swapped
                y = x.getParent(cache).getParent(cache).getLeft(cache);
                if(y!= null && y.getColor(cache) == Color.RED) {
                    x.getParent(cache).setColor(cache, Color.BLACK);
                    y.setColor(cache, Color.BLACK);
                    x.getParent(cache).getParent(cache).setColor(cache, Color.RED);
                    x						= x.getParent(cache).getParent(cache);
                } else {
                    if(x == x.getParent(cache).getLeft(cache)) {
                        x = x.getParent(cache);
                        rotateRight(cache, x);
                    }
                    x.getParent(cache).setColor(cache, Color.BLACK);
                    x.getParent(cache).getParent(cache).setColor(cache, Color.RED);
                    rotateLeft(cache, x.getParent(cache).getParent(cache));
                }
            }
        }
        getRoot(cache).setColor(cache, Color.BLACK);		// root should always be black
    }

    public void rotateLeft(CacheWrapper cache, RBNode x) {
        // pushing node x down and to the Left to balance the tree. x's Right child (y)
        // replaces x (since y > x), and y's Left child becomes x's Right child
        // (since it's < y but > x).

        RBNode y = x.getRight(cache); // get x's Right node, this becomes y

        // set x's Right link
        x.setRight(cache, y.getLeft(cache));	// y's Left child's becomes x's Right child

        // modify parents
        if(!y.getLeft(cache).isSentinel())
            y.getLeft(cache).setParent(cache, x);		// sets y's Left Parent to x

        if(!y.isSentinel())
            y.setParent(cache, x.getParent(cache));	// set y's Parent to x's Parent

        if(x.getParent(cache) != null) {	// determine which side of it's Parent x was on
            if(x == x.getParent(cache).getLeft(cache))
                x.getParent(cache).setLeft(cache, y);			// set Left Parent to y
            else
                x.getParent(cache).setRight(cache, y);			// set Right Parent to y
        } else
            setRoot(cache, y);						// at root, set it to y

        // link x and y
        y.setLeft(cache, x);			// put x on y's Left
        if(!x.isSentinel())		// set y as x's Parent
            x.setParent(cache, y);
    }

    public void rotateRight(CacheWrapper cache, RBNode x) {
        // pushing node x down and to the Right to balance the tree. x's Left child (y)
        // replaces x (since x < y), and y's Right child becomes x's Left child
        // (since it's < x but > y).

        RBNode y = x.getLeft(cache);			// get x's Left node, this becomes y

        // set x's Right link
        x.setLeft(cache, y.getRight(cache));			// y's Right child becomes x's Left child

        // modify parents
        if(!y.getRight(cache).isSentinel())
            y.getRight(cache).setParent(cache, x);		// sets y's Right Parent to x

        if(!y.isSentinel())
            y.setParent(cache, x.getParent(cache));			// set y's Parent to x's Parent

        if(x.getParent(cache) != null)				// null=root, could also have used root
        {	// determine which side of its Parent x was on
            if(x == x.getParent(cache).getRight(cache))
                x.getParent(cache).setRight(cache, y);			// set Right Parent to y
            else
                x.getParent(cache).setLeft(cache, y);			// set Left Parent to y
        } else
            setRoot(cache, y);						// at root, set it to y

        // link x and y
        y.setRight(cache, x);					// put x on y's Right
        if(!x.isSentinel())				// set y as x's Parent
            x.setParent(cache, y);
    }

    private int count(CacheWrapper cache, RBNode root) {
        if (root.isSentinel())
            return 0;
        return 1 + count(cache, root.getLeft(cache)) + count(cache, root.getRight(cache));
    }

    private void recursiveValidate(CacheWrapper cache, RBNode root, int blackNodes, int soFar) {
        // Empty sub-tree is vacuously OK
        if (root.isSentinel())
            return;

        Color rootcolor = root.getColor(cache);
        soFar += ((Color.BLACK == rootcolor) ? 1 : 0);
        root.setMarked(cache, true);

        // Check left side
        RBNode left = root.getLeft(cache);
        if (!left.isSentinel()) {
            if (left.getColor(cache) != Color.RED || rootcolor != Color.RED) {
                System.out.println("Error: Two consecutive red nodes!");
            }
            if (left.getValue(cache) < root.getValue(cache)) {
                System.out.println(" Error; Tree values out of order!");
            }
            if (!left.isMarked(cache)) {
                System.out.println("Error; Cycle in tree structure!");
            }
            recursiveValidate(cache, left, blackNodes, soFar);
        }

        // Check right side
        RBNode right = root.getRight(cache);
        if (!right.isSentinel()) {
            if (right.getColor(cache) != Color.RED || rootcolor != Color.RED) {
                System.out.println("Error: Two consecutive red nodes!");
            }
            if (right.getValue(cache) > root.getValue(cache)) {
                System.out.println("Error: Tree values out of order!");
            }
            if (!right.isMarked(cache)) {
                System.out.println("Error: Cycle in tree structure!");
            }
            recursiveValidate(cache, right, blackNodes, soFar);
        }

        // Check black node count
        if (root.getLeft(cache).isSentinel() || root.getRight(cache).isSentinel()) {
            if (soFar != blackNodes) {
                System.out.println("Error: Variable number of black nodes to leaves!");
                return;
            }
        }
        // Everything checks out if we get this far.
        return;
    }

    public class RBNode implements Serializable {

        private String uuid;
        private boolean sentinel;

        public RBNode() {
            this.uuid = UUID.randomUUID().toString();
        }
        
        public RBNode(boolean sentinel) {
            this();
            this.sentinel = sentinel;
        }
        
        public boolean isSentinel() {
            return this.sentinel;
        }
        
        public int getValue(CacheWrapper wrapper){
            LocatedKey key = wrapper.createKey(uuid + ":value", myNode);
            return (Integer) Micro.get(wrapper, key);
        }

        public void setValue(CacheWrapper wrapper, int newValue){
            LocatedKey key = wrapper.createKey(uuid + ":value", myNode);
            Micro.put(wrapper, key, newValue);
        }

        public boolean isMarked(CacheWrapper wrapper){
            LocatedKey key = wrapper.createKey(uuid + ":isMarked", myNode);
            return (Boolean) Micro.get(wrapper, key);
        }

         public void setMarked(CacheWrapper wrapper, boolean newMarked){
             LocatedKey key = wrapper.createKey(uuid + ":isMarked", myNode);
             Micro.put(wrapper, key, newMarked);
         }

         public Color getColor(CacheWrapper wrapper){
             LocatedKey key = wrapper.createKey(uuid + ":color", myNode);
             return (Color) Micro.get(wrapper, key);
         }

         public void setColor(CacheWrapper wrapper, Color newColor){
             LocatedKey key = wrapper.createKey(uuid + ":color", myNode);
             Micro.put(wrapper, key, newColor);
         }

         public RBNode getParent(CacheWrapper wrapper){
             LocatedKey key = wrapper.createKey(uuid + ":parent", myNode);
             return (RBNode) Micro.get(wrapper, key);
         }

         public void setParent(CacheWrapper wrapper, RBNode newParent){
             LocatedKey key = wrapper.createKey(uuid + ":parent", myNode);
             Micro.put(wrapper, key, newParent);
         }

         public RBNode getLeft(CacheWrapper wrapper){
             LocatedKey key = wrapper.createKey(uuid + ":left", myNode);
             return (RBNode) Micro.get(wrapper, key);
         }

         public void setLeft(CacheWrapper wrapper, RBNode newLeft){
             LocatedKey key = wrapper.createKey(uuid + ":left", myNode);
             Micro.put(wrapper, key, newLeft);
         }

         public RBNode getRight(CacheWrapper wrapper){
             LocatedKey key = wrapper.createKey(uuid + ":right", myNode);
             return (RBNode) Micro.get(wrapper, key);
         }

         public void setRight(CacheWrapper wrapper, RBNode newRight){
             LocatedKey key = wrapper.createKey(uuid + ":right", myNode);
             Micro.put(wrapper, key, newRight);
         }

    }
}
