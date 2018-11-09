﻿namespace Sitecore.Support.ContentSearch.LuceneProvider
{
  using System;
  using System.Collections.Concurrent;
  using Sitecore.ContentSearch;

  public class LuceneDocumentBuilder : Sitecore.ContentSearch.LuceneProvider.LuceneDocumentBuilder
  {
    public LuceneDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
    {

    }

    protected override void AddItemFields()
    {
      try
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");

        //fix for 230603
        //if (this.Options.IndexAllFields)
        //{
        this.Indexable.LoadAllFields();
        //}
        /* fix for 230603
        var loadedFields = new HashSet<string>(this.Indexable.Fields.Select(f => f.Id.ToString()));
        var includedFields = new HashSet<string>();
        if (this.Options.HasIncludedFields)
        {
          includedFields = new HashSet<string>(this.Options.IncludedFields);
        }
        includedFields.ExceptWith(loadedFields);
        */
        if (IsParallel)
        {
          var exceptions = new ConcurrentQueue<Exception>();

          this.ParallelForeachProxy.ForEach(this.Indexable.Fields, this.ParallelOptions, f =>
          {
            try
            {
              this.CheckAndAddField(this.Indexable, f);
            }
            catch (Exception ex)
            {
              exceptions.Enqueue(ex);
            }
          });

          if (exceptions.Count > 0)
          {
            throw new AggregateException(exceptions);
          }
          /* fix for 230603
          if (!this.Options.IndexAllFields && this.Options.HasIncludedFields)
          {
            this.ParallelForeachProxy.ForEach(includedFields, this.ParallelOptions, fieldId =>
            {
              try
              {
                ID id;
                if (ID.TryParse(fieldId, out id))
                {
                  var field = this.Indexable.GetFieldById(id);
                  if (field != null)
                  {
                    this.CheckAndAddField(this.Indexable, field);
                  }
                }
              }
              catch (Exception ex)
              {
                exceptions.Enqueue(ex);
              }
            });
          }
          */
        }
        else
        {
          foreach (var field in this.Indexable.Fields)
          {
            this.CheckAndAddField(this.Indexable, field);
          }
          /* fix for 230603
          if (!this.Options.IndexAllFields && this.Options.HasIncludedFields)
          {
            foreach (var fieldId in includedFields)
            {
              ID id;
              if (ID.TryParse(fieldId, out id))
              {
                var field = this.Indexable.GetFieldById(id);
                if (field != null)
                {
                  this.CheckAndAddField(this.Indexable, field);
                }
              }
            }
          }
          */
        }
      }
      finally
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
      }
    }
  }
}