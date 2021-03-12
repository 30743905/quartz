package org.simon.test02;

import org.junit.Test;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;

/**
 * @author Administrator
 * @Copyright © 2020 tiger Inc. All rights reserved.
 * @create 2020-12-14 22:52
 */
public class Demo01 {

	@Test
	public void test01() throws IntrospectionException {
		Person p = new Person();
		BeanInfo beanInfo = Introspector.getBeanInfo(p.getClass());
		PropertyDescriptor[] pds = beanInfo.getPropertyDescriptors();
		for(PropertyDescriptor pd:pds) {
			System.out.println(pd.getName());
		}
	}

}