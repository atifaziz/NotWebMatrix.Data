#region Copyright (c) 2012 Atif Aziz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#endregion

namespace NotWebMatrix.Data
{
    #region Imports

    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.ComponentModel;
    using System.Data;
    using System.Dynamic;
    using System.Linq;

    #endregion

    /// <summary>
    /// Represents a data record by using a custom type descriptor and the
    /// capabilities of the Dynamic Language Runtime (DLR).
    /// </summary>

    [Obsolete]
    public sealed class DynamicRecord : DynamicObject, ICustomTypeDescriptor
    {
        readonly IDataRecord _record;
        ReadOnlyCollection<string> _columns;

        public DynamicRecord(IDataRecord record) =>
            _record = record ?? throw new ArgumentNullException(nameof(record));

        public object this[string name]      => GetValue(_record[name]);
        public object this[int index]        => GetValue(_record[index]);
        static object GetValue(object value) => !Convert.IsDBNull(value) ? value : null;

        public IList<string> Columns
        {
            get
            {
                if (_columns == null)
                {
                    var record = _record;
                    var names = from i in Enumerable.Range(0, record.FieldCount)
                                select record.GetName(i);
                    _columns = Array.AsReadOnly(names.ToArray());
                }
                return _columns;
            }
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = this[binder.Name];
            return true;
        }

        public override IEnumerable<string> GetDynamicMemberNames() { return Columns; }

        #region ICustomTypeDescriptor

        AttributeCollection ICustomTypeDescriptor.GetAttributes() => AttributeCollection.Empty;
        string ICustomTypeDescriptor.GetClassName() => null;
        string ICustomTypeDescriptor.GetComponentName() => null;
        TypeConverter ICustomTypeDescriptor.GetConverter() => null;
        EventDescriptor ICustomTypeDescriptor.GetDefaultEvent() => null;
        PropertyDescriptor ICustomTypeDescriptor.GetDefaultProperty() => null;
        object ICustomTypeDescriptor.GetEditor(Type editorBaseType) => null;
        EventDescriptorCollection ICustomTypeDescriptor.GetEvents(Attribute[] attributes) => EventDescriptorCollection.Empty;
        EventDescriptorCollection ICustomTypeDescriptor.GetEvents() => EventDescriptorCollection.Empty;
        PropertyDescriptorCollection ICustomTypeDescriptor.GetProperties(Attribute[] attributes) => ((ICustomTypeDescriptor)this).GetProperties();
        object ICustomTypeDescriptor.GetPropertyOwner(PropertyDescriptor pd) => this;

        PropertyDescriptorCollection ICustomTypeDescriptor.GetProperties()
        {
            var record = _record;
            var columns = Columns;
            var properties =
                from i in Enumerable.Range(0, columns.Count)
                select (PropertyDescriptor)new DynamicPropertyDescriptor(columns[i], record.GetFieldType(i));
            return new PropertyDescriptorCollection(properties.ToArray(), readOnly: true);
        }

        #endregion

        class DynamicPropertyDescriptor : PropertyDescriptor
        {
            public DynamicPropertyDescriptor(string name, Type type) :
                base(name, Array.Empty<Attribute>()) =>
                PropertyType = type;

            public override object GetValue(object component) =>
                component is DynamicRecord record ? record[Name] : null;

            public override Type ComponentType => typeof(DynamicRecord);
            public override bool IsReadOnly => true;
            public override Type PropertyType { get; }
            public override bool CanResetValue(object component) => false;
            public override void ResetValue(object component) => throw CreateRecordReadOnlyException();
            public override void SetValue(object component, object value) => throw CreateRecordReadOnlyException();
            public override bool ShouldSerializeValue(object component) => false;

            Exception CreateRecordReadOnlyException() =>
                new InvalidOperationException($@"Unable to modify the value of column ""{Name}"" because the record is read only.");
        }
    }
}
