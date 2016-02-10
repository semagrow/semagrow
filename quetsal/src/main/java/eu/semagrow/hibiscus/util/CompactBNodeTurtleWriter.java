package eu.semagrow.hibiscus.util;

/*
 * This file is part of RDF Federator.
 * Copyright 2010 Olaf Goerlitz
 *
 * RDF Federator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * RDF Federator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with RDF Federator.  If not, see <http://www.gnu.org/licenses/>.
 *
 * RDF Federator uses libraries from the OpenRDF Sesame Project licensed
 * under the Aduna BSD-style license.
 */

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.turtle.TurtleWriter;

/**
 * Created by antru on 21/4/2015.
 */


/**
 * Writes compact turtle syntax using the [] shorthand notation if a BNode
 * occurs as object in a triple statement followed by triple statements with
 * the same BNode in subject position.
 *
 * Beware: if the [] syntax is applied the BNode MUST occur in consecutive
 *         triple statements. Otherwise, the link between individual statement
 *         groups would be lost due to the omission of the the BNode ID.
 *
 * @author Olaf Goerlitz
 */
public class CompactBNodeTurtleWriter extends TurtleWriter {

    protected BNode pendingBNode;
    protected Deque<Resource> storedSubjects = new ArrayDeque<Resource>();
    protected Deque<URI> storedPredicates = new ArrayDeque<URI>();
    protected Deque<Value> storedBNodes = new ArrayDeque<Value>();
    protected Set<BNode> seenBNodes = new HashSet<BNode>();

    /**
     * Creates a new TurtleWriter that will write to the supplied OutputStream.
     *
     * @param out The OutputStream to write the Turtle document to.
     */
    public CompactBNodeTurtleWriter(OutputStream out) {
        super(out);
    }

    /**
     * Creates a new TurtleWriter that will write to the supplied Writer.
     *
     * @param writer The Writer to write the Turtle document to.
     */
    public CompactBNodeTurtleWriter(Writer writer) {
        super(writer);
    }

    // -------------------------------------------------------------------------

    /**
     * Special handling of previous BNode object - try to use '[ ... ]'
     * Open [] block if current subject is the same BNode,
     * else finish last triple.
     *
     * @param subj the current subject
     */
    private void handlePendingBNode(Resource subj) throws IOException {

        // check if last triple's object was a BNode object
        if (pendingBNode != null) {

            // if current triple's subject is the same BNode use [] notation
            if (subj.equals(pendingBNode)) {

                // first, save the internal status to resume an open [] block
                storedBNodes.push(pendingBNode);
                storedSubjects.push(lastWrittenSubject);
                storedPredicates.push(lastWrittenPredicate);

                // then open the new [] block and update the internal status
                writer.write("[");
                writer.writeEOL();
                writer.increaseIndentation();
                lastWrittenSubject = pendingBNode;
                lastWrittenPredicate = null;

            } // otherwise write pending BNode object
            else {
                writeValue(pendingBNode);
            }
            pendingBNode = null;
        }
    }

    /**
     * Handles a currently open [] block.
     * @param subj the current subject
     * @throws java.io.IOException
     */
    private void handleActiveBNode(Resource subj) throws IOException {

        // check if a [] block is currently open
        if (storedBNodes.size() > 0) {

            // check if [] block has to be closed due to changed subject
            if (!subj.equals(storedBNodes.peek())) {

                // close last statement and finish [] block
                writer.write(" ;");
                writer.writeEOL();
                writer.decreaseIndentation();
                writer.write("]");

                // resume last internal status before [] block
                statementClosed = false;
                storedBNodes.pop();
                lastWrittenSubject = storedSubjects.pop();
                lastWrittenPredicate = storedPredicates.pop();
            }
        }
    }


    // -------------------------------------------------------------------------

    /**
     * Handles a statement.
     *
     * @param st The statement.
     * @throws org.openrdf.rio.RDFHandlerException
     *         If the RDF handler has encountered an unrecoverable error.
     */
    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        if (!writingStarted) {
            throw new RuntimeException("Document writing has not yet been started");
        }

        Resource subj = st.getSubject();
        URI pred = st.getPredicate();
        Value obj = st.getObject();

        try {

            // check for pending BNode:
            // i.e. last triple's object was a BNode and not written yet
            handlePendingBNode(subj);
            handleActiveBNode(subj);

            // check if subject (and predicate) are identical with last triple
            if (subj.equals(lastWrittenSubject)) {

                // "," for identical subject and predicate
                if (pred.equals(lastWrittenPredicate)) {
                    // Identical subject and predicate
                    writer.write(" , ");
                }

                // ";" for identical subject with different predicate
                // unless last predicate is null (new [] block)
                else {

                    if (lastWrittenPredicate != null) {
                        writer.write(" ;");
                        writer.writeEOL();
                    }

                    // Write new predicate
                    writePredicate(pred);
                    writer.write(" ");
                    lastWrittenPredicate = pred;
                }
            }
            else {
                // New subject
                closePreviousStatement();

                // Write new subject:
                writer.writeEOL();
                writeResource(subj);
                writer.write(" ");
                lastWrittenSubject = subj;

                // Write new predicate
                writePredicate(pred);
                writer.write(" ");
                lastWrittenPredicate = pred;

                statementClosed = false;
                writer.increaseIndentation();
            }

            // defer BNode object writing until next statement is checked
            if (obj instanceof BNode) {
                pendingBNode = (BNode) obj;

                // check if this BNode has been processed before
                // if so the previous id was lost due to the shorthand notation
                // writing it again would result in two distinct BNodes
                if (seenBNodes.contains(pendingBNode)) {
                    throw new IllegalStateException("Same BNode may occur only once in object position: " + st);
                } else {
                    seenBNodes.add(pendingBNode);
                }
            } else {
                writeValue(obj);
            }

            // Don't close the line yet. The next triple statement may have
            // the same subject and/or predicate.
        }
        catch (IOException e) {
            throw new RDFHandlerException(e);
        }
    }

    public void endRDF() throws RDFHandlerException {
        if (!writingStarted) {
            throw new RuntimeException("Document writing has not yet started");
        }

        try {
            // finish open statements and BNode blocks
            if (storedBNodes.size() == 0) {
                closePreviousStatement();
            } else {
                for (int i = 0; i < storedBNodes.size(); i++) {
                    if (!statementClosed) {
                        writer.write(" .");
                        statementClosed = true;
                    }
                    writer.writeEOL();
                    writer.decreaseIndentation();
                    writer.write("] .");
                }
            }
            writer.flush();
        }
        catch (IOException e) {
            throw new RDFHandlerException(e);
        }
        finally {
            writingStarted = false;
        }
    }

    public static void main(String[] args) {

        ValueFactory vf = ValueFactoryImpl.getInstance();
        Writer writer = new StringWriter();
        RDFWriter rdf = new CompactBNodeTurtleWriter(writer);

        try {
            rdf.startRDF();

            // add namespaces which will be automatically shortened
            rdf.handleNamespace("owl", "http://www.w3.org/2002/07/owl#");
            rdf.handleNamespace("xsd", "http://www.w3.org/2001/XMLSchema#");
            rdf.handleNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
            rdf.handleNamespace("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
            rdf.handleNamespace("test", "http://test.org/");

            // _:#1 rdfs:label "Thing 1"; a test:Thing; owl:sameAs
            // [ rdfs:label "Thing 2"; a test:Thing; owl:sameAs _:#1,
            // [ rdfs:label "Thing 3"; a test:Thing; ]] .
            BNode thing = vf.createBNode();
            BNode sameThing = vf.createBNode();
            BNode otherThing = vf.createBNode();
            rdf.handleStatement(vf.createStatement(thing, RDFS.LABEL, vf.createLiteral("1")));
            rdf.handleStatement(vf.createStatement(thing, RDF.TYPE, vf.createURI("http://test.org/Thing")));
            rdf.handleStatement(vf.createStatement(thing, OWL.SAMEAS, sameThing));
            rdf.handleStatement(vf.createStatement(sameThing, RDFS.LABEL, vf.createLiteral(1l)));
            rdf.handleStatement(vf.createStatement(sameThing, RDF.TYPE, vf.createURI("http://test.org/Thing")));
            rdf.handleStatement(vf.createStatement(sameThing, OWL.SAMEAS, thing));
            rdf.handleStatement(vf.createStatement(sameThing, OWL.SAMEAS, otherThing));
            rdf.handleStatement(vf.createStatement(otherThing, RDFS.LABEL, vf.createLiteral(1d)));
            rdf.handleStatement(vf.createStatement(otherThing, RDF.TYPE, vf.createURI("http://test.org/Thing")));

            rdf.endRDF();
        } catch (RDFHandlerException e) {
            e.printStackTrace();
        } catch (UnsupportedOperationException e) {
            e.printStackTrace();
        }

        System.out.println(writer.toString());
    }

}

