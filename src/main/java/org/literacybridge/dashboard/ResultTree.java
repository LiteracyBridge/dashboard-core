package org.literacybridge.dashboard;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to easily track hierarchical results, like project / deployment / tbloader id / village / talking book.
 * 
 * Items are strictly strings. A path through the hierarchy is given by a List of Strings.
 * 
 * Can add key:value attributes to any item in the tree.
 *
 * Very simple report generation prints nested lists, as plain text or html.
 */
public class ResultTree {
    protected final String name;

    // Lazy create these
    private Map<String, String> attributes = null;
    private Map<String, ResultTree> children = null;

    protected ResultTree(String name) {
        this.name = name;
    }

    /**
     * Add a child to a node, or return it if already present.
     * @param childName Name to be added.
     * @return The new or existing node.
     */
    protected ResultTree addChild(String childName) {
        // If not first child, may already have this one.
        ResultTree child = null;
        if (children == null) {
            children = new LinkedHashMap<>();
        } else {
            child = children.get(childName);
        }
        if (child == null) {
            child = new ResultTree(childName);
            children.put(childName, child);
        }
        return child;
    }

    /**
     * Add a child to a node, or return it if already present.
     * @param path List of Names of node to be added.
     * @return The new or existing node.
     */
    protected ResultTree addChild(List<String> path) {
        // If the direct child does not already exist, creates it.
        ResultTree child = addChild(path.get(0));
        // Then the child's children
        if (path.size() > 1) {
            return child.addChild(path.subList(1, path.size()));
        }
        return child;
    }

    /**
     * Adds a key:value attribute to a node.
     * @param key The key to be added or updated.
     * @param value The new value.
     */
    protected void addAttribute(String key, String value) {
        if (attributes == null) {
            attributes = new LinkedHashMap<>();
        }
        attributes.put(key, value);
    }
    /**
     * Adds a key:value attribute to a node.
     * @param path List of Names of node to receive the attribute.
     * @param key The key to be added or updated.
     * @param value The new value.
     */
    protected void addAttribute(List<String> path, String key, String value) {
        addChild(path).addAttribute(key, value);
    }

    /**
     * Creates a formatted string of the node, attributes, and children.
     * @param indent How much to indent each line.
     * @return The node, formatted as a string.
     */
    private String makeString(int indent) {
        String prefix = "";
        if (indent > 0) {
            prefix = String.format("%1$" + indent * 2 + "s", " ");
        }
        String prefix2 = "  " + prefix;
        StringBuilder result = new StringBuilder(prefix).append(name).append("\n");
        if (attributes != null) {
            for (Map.Entry<String, String> e : attributes.entrySet()) {
                result.append(prefix2).append(e.getKey()).append(" : ").append(e.getValue()).append("\n");
            }
        }
        if (children != null) {
            for (ResultTree rt : children.values()) {
                result.append(rt.makeString(indent+1));
            }
        }
        return result.toString();
    }
    
    protected String makeString() {
        return makeString(0);
    }

    /**
     * Turns a key like "LogFileErrors" or "foo bar" into better CSS class names, "logfileerrors" or
     * "foo-bar".
     * @param raw name to be made into a better CSS class name.
     * @return the better name.
     */
    private String makeClassy(String raw) {
        return raw.replace(' ', '-').toLowerCase();
    }

    private String makeHtml(int level) {
        StringBuilder result = new StringBuilder(String.format("<h%d>%s</h%d>", level, name, level));
        if (attributes != null) {
            result.append("<ul>");
            for (Map.Entry<String, String> e : attributes.entrySet()) {
                result.append(String.format("<li class='%s'>%s : %s</li>", makeClassy(e.getKey()), e.getKey(), e.getValue()));
            }
            result.append("</ul>");
        }
        if (children != null) {
            result.append("<ul>");
            for (ResultTree rt : children.values()) {
                result.append("<li>").append(rt.makeHtml(level+1)).append("</li>");
            }
            result.append("</ul>");
        }
        return result.toString();
    }
    protected String makeHtml() {
        String reportClass = reportClass();
        String report = makeHtml(2);
        if (reportClass != null && reportClass.length() > 0) {
            report = String.format("<div class='%s'>%s</div>", reportClass, report);
        }
        return report;
    }

    /**
     * Prints a plain-text report to the given PrintStream.
     * @param ps Where to print the report.
     */
    public void report(PrintStream ps) {
        report(ps, false);
    }

    /**
     * Print a report to the given file. If the name ends with ".html", generate html.
     *
     * @param file Where to print the report.
     */
    public void report(File file) {
        try {
            boolean html = file.getName().toLowerCase().endsWith(".html");
            // If the file exists, append a new report to it.
            FileOutputStream fos = new FileOutputStream(file, true);
            PrintStream ps = new PrintStream(fos);

            report(ps, html);
        } catch (IOException e) {
            // Couldn't create the file?
        }
    }

    /**
     * Prints a report to the given PrintStream.
     * @param ps Where to print the report.
     * @param html Use HTML format?
     */
    public void report(PrintStream ps, boolean html) {
        String report = html ? makeHtml() : makeString();

        ps.print(report);
        ps.flush();
    }

    /**
     * Override this to set "class='foo'" on the report.
     * @return
     */
    protected String reportClass() {
        return "";
    }

}
