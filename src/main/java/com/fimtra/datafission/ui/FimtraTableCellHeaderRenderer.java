/*
 * Copyright (c) 2014 James Lupton, Ramon Servadei, Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.datafission.ui;

/**
 * @author James
 *
 * This is a copy of the DefaultTableCellHeaderRenderer with a default tooltip and 
 * we get the UI values from the UI manager directly.
 * The DefaultTableCellHeaderRenderer is in a restricted package which we don't want
 * to add here.
 */
import java.awt.Component;
import java.awt.Color;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Insets;
import java.awt.Point;
import java.awt.Rectangle;
import java.io.Serializable;

import javax.swing.*;
import javax.swing.plaf.UIResource;
import javax.swing.border.Border;
import javax.swing.table.*;

public class FimtraTableCellHeaderRenderer extends DefaultTableCellRenderer
        implements UIResource {
	private static final long serialVersionUID = 1L;
	
	private boolean horizontalTextPositionSet;
    private Icon sortArrow;
    private EmptyIcon emptyIcon = new EmptyIcon();

    public FimtraTableCellHeaderRenderer() {
        setHorizontalAlignment(JLabel.CENTER);
    }

    public void setHorizontalTextPosition(int textPosition) {
        horizontalTextPositionSet = true;
        super.setHorizontalTextPosition(textPosition);
    }

    public Component getTableCellRendererComponent(JTable table, Object value,
            boolean isSelected, boolean hasFocus, int row, int column) {
        Icon sortIcon = null;

        boolean isPaintingForPrint = false;

        if (table != null) {
            JTableHeader header = table.getTableHeader();
            if (header != null) {
                Color fgColor = null;
                Color bgColor = null;
                if (hasFocus) {
                    fgColor = getUIColor(this, "TableHeader.focusCellForeground");
                    bgColor = getUIColor(this, "TableHeader.focusCellBackground");
                }
                if (fgColor == null) {
                    fgColor = header.getForeground();
                }
                if (bgColor == null) {
                    bgColor = header.getBackground();
                }
                setForeground(fgColor);
                setBackground(bgColor);

                setFont(header.getFont());

                isPaintingForPrint = header.isPaintingForPrint();
            }

            if (!isPaintingForPrint && table.getRowSorter() != null) {
                if (!horizontalTextPositionSet) {
                    // There is a row sorter, and the developer hasn't
                    // set a text position, change to leading.
                    setHorizontalTextPosition(JLabel.LEADING);
                }
                SortOrder sortOrder = getColumnSortOrder(table, column);
                if (sortOrder != null) {
                    switch(sortOrder) {
                    case ASCENDING:
                        sortIcon = getUIIcon(
                            this, "Table.ascendingSortIcon");
                        break;
                    case DESCENDING:
                        sortIcon = getUIIcon(
                            this, "Table.descendingSortIcon");
                        break;
                    case UNSORTED:
                        sortIcon = getUIIcon(
                            this, "Table.naturalSortIcon");
                        break;
                    }
                }
            }
        }

        setText(value == null ? "" : value.toString());
        setIcon(sortIcon);
        sortArrow = sortIcon;

        Border border = null;
        if (hasFocus) {
            border = getUIBorder(this, "TableHeader.focusCellBorder");
        }
        if (border == null) {
            border = getUIBorder(this, "TableHeader.cellBorder");
        }
        setBorder(border);

        return this;
    }
    
    @Override
    public String getToolTipText() {
    	return getText();
    }

    public static SortOrder getColumnSortOrder(JTable table, int column) {
        SortOrder rv = null;
        if (table == null || table.getRowSorter() == null) {
            return rv;
        }
        java.util.List<? extends RowSorter.SortKey> sortKeys =
            table.getRowSorter().getSortKeys();
        if (sortKeys.size() > 0 && sortKeys.get(0).getColumn() ==
            table.convertColumnIndexToModel(column)) {
            rv = sortKeys.get(0).getSortOrder();
        }
        return rv;
    }

    @Override
    public void paintComponent(Graphics g) {
        boolean b = getUIBoolean(this, 
                "TableHeader.rightAlignSortArrow", false);
        if (b && sortArrow != null) {
            //emptyIcon is used so that if the text in the header is right
            //aligned, or if the column is too narrow, then the text will
            //be sized appropriately to make room for the icon that is about
            //to be painted manually here.
            emptyIcon.width = sortArrow.getIconWidth();
            emptyIcon.height = sortArrow.getIconHeight();
            setIcon(emptyIcon);
            super.paintComponent(g);
            Point position = computeIconPosition(g);
            sortArrow.paintIcon(this, g, position.x, position.y);
        } else {
            super.paintComponent(g);
        }
    }

    private Point computeIconPosition(Graphics g) {
        FontMetrics fontMetrics = g.getFontMetrics();
        Rectangle viewR = new Rectangle();
        Rectangle textR = new Rectangle();
        Rectangle iconR = new Rectangle();
        Insets i = getInsets();
        viewR.x = i.left;
        viewR.y = i.top;
        viewR.width = getWidth() - (i.left + i.right);
        viewR.height = getHeight() - (i.top + i.bottom);
        SwingUtilities.layoutCompoundLabel(
            this,
            fontMetrics,
            getText(),
            sortArrow,
            getVerticalAlignment(),
            getHorizontalAlignment(),
            getVerticalTextPosition(),
            getHorizontalTextPosition(),
            viewR,
            iconR,
            textR,
            getIconTextGap());
        int x = getWidth() - i.right - sortArrow.getIconWidth();
        int y = iconR.y;
        return new Point(x, y);
    }
    
    private static Color getUIColor(JComponent c, String key){
    	Object ob = UIManager.get(key, c.getLocale());
    	if(ob == null || !(ob instanceof Color)){
    		return null;
    	}
    	return (Color)ob;
    }

    private static Icon getUIIcon(JComponent c, String key) {
        Object iValue = UIManager.get(key, c.getLocale());
        if (iValue == null || !(iValue instanceof Icon)) {
            return null;
        }
        return (Icon)iValue;
    }
    
    private static Border getUIBorder(JComponent c, String key) {
        Object iValue = UIManager.get(key, c.getLocale());
        if (iValue == null || !(iValue instanceof Border)) {
            return null;
        }
        return (Border)iValue;
    }
    
	private static boolean getUIBoolean(JComponent c, String key,
			boolean defaultValue) {
		Object iValue = UIManager.get(key, c.getLocale());

		if (iValue == null || !(iValue instanceof Boolean)) {
			return defaultValue;
		}
		return ((Boolean) iValue).booleanValue();
	}
    
    private class EmptyIcon implements Icon, Serializable {
        int width = 0;
        int height = 0;
        public void paintIcon(Component c, Graphics g, int x, int y) {}
        public int getIconWidth() { return width; }
        public int getIconHeight() { return height; }
    }
}

